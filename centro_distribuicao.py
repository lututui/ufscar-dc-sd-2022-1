import threading

from gevent import monkey

monkey.patch_all()

import bottle as bottle
import paho.mqtt.client as mqtt
import schedule as schedule
from tabulate import tabulate
from gevent import sleep as g_sleep

import util
from Estoque import Estoque

c = None


class Fabrica:
    def __init__(self, fid):
        self.id = fid
        self.produtos = []


class Loja:
    def __init__(self, lid):
        self.id = lid


class CentroDistribuicao:
    def __init__(self):
        self.estoque = Estoque(centro=True)
        self.fabricas: dict[str, Fabrica] = {}
        self.lojas: dict[str, Loja] = {}

        self.ready = False

        self.dia = 0
        self.buffer_update = {}

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_subscribe = self.on_subscribe

    def heartbeat_lojas(self):
        lojas_conectadas = list(self.lojas.keys())

        if len(lojas_conectadas) >= 20:
            print('Recebido informação de todas as lojas')
            self.ready = True
            return schedule.CancelJob

        print('Requisitando heartbeat de lojas')

        self.lojas.clear()

        self.mqtt_client.publish(
            util.topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.LOJA, {})
        )

    def heartbeat_fabricas(self):
        fabricas_conectadas = list(self.fabricas.keys())

        if len(fabricas_conectadas) >= 70:
            print('Recebido informação de todas as fabricas')
            print('Pedindo lojas pela primeira vez')
            self.heartbeat_lojas()

            schedule.every(15).seconds.do(self.heartbeat_lojas)
            return schedule.CancelJob

        print('Requisitando heartbeat de fábricas')

        # print(self.fabricas)

        self.fabricas.clear()

        self.mqtt_client.publish(
            util.topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.FABRICA, {})
        )

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        self.mqtt_client.subscribe(util.topic)

    def update_lojas(self):
        if len(self.buffer_update) == 20:
            print('Recebeu todos os updates')

            for lid in self.buffer_update.keys():
                pedido_loja = self.buffer_update[lid]

                for produto in pedido_loja.keys():
                    qntd = pedido_loja[produto]

                    print(f'Loja {lid} pediu reestoque de {produto} x {qntd} no dia {self.dia}')

                    self.estoque.debito(produto, qntd)

                print(f'Processou pedido da loja {lid}')

                if len(pedido_loja) > 0:
                    print(f'Enviando reestoque para loja {lid}')

                    self.mqtt_client.publish(
                        util.topic,
                        payload=util.build_msg(
                            util.MsgType.RESTOCK,
                            util.MsgTargetType.LOJA,
                            pedido_loja,
                            msg_target_id=lid
                        )
                    )

            return schedule.CancelJob

        self.buffer_update = {}

        print(f'[Dia {self.dia}] Enviando pedido de update para todas as lojas')

        self.mqtt_client.publish(
            util.topic,
            payload=util.build_msg(
                util.MsgType.UPDATE,
                util.MsgTargetType.LOJA,
                msg_payload={'dia': self.dia}
            )
        )

    def on_message(self, client, userdata, msg):
        payload = util.decode_msg(msg.payload)

        if payload is None:
            return

        if payload['target'] != util.MsgTargetType.CENTRO_DISTRIBUICAO:
            return

        if payload['type'] == util.MsgType.GET:
            # Fábrica
            if 'fid' in payload['msg']:
                # print(payload['msg'])
                uuid = payload['msg']['fid']
                self.fabricas[uuid] = Fabrica(uuid)
                f_len = len(self.fabricas)

                self.mqtt_client.publish(
                    util.topic,
                    payload=util.build_msg(
                        util.MsgType.SET,
                        util.MsgTargetType.FABRICA,
                        {"produtos": self.estoque.tipos_produtos[f_len - 1:f_len + 2]},
                        msg_target_id=uuid
                    )
                )

                print(f'Fábricas identificadas: {len(self.fabricas)}/70')
                return

            if 'lid' in payload['msg']:
                uuid = payload['msg']['lid']
                self.lojas[uuid] = Loja(uuid)

                print(f'Lojas identificadas: {len(self.lojas)}/20')
                return

            return

        if payload['type'] == util.MsgType.UPDATE:
            if payload['msg']['dia'] != self.dia:
                return

            if payload['msg']['lid'] in self.buffer_update:
                return

            self.buffer_update[payload['msg']['lid']] = payload['msg']['update']
            print(f'Recv update [Dia {self.dia}]: {len(self.buffer_update)}/20')
            return

        print(f'Centro de distribuição recv unparsed msg {msg.payload.decode()}')

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print('Pedindo fábricas pela primeira vez')
        self.heartbeat_fabricas()

        schedule.every(15).seconds.do(self.heartbeat_fabricas)


def main(centro: CentroDistribuicao):
    centro.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    centro.mqtt_client.loop_start()

    try:
        while True:
            schedule.run_pending()
    except KeyboardInterrupt:
        pass

    centro.mqtt_client.loop_stop()


@bottle.route('/', method='POST')
def home_post():
    step = bottle.request.forms.get('step') == 'step'

    if step:
        print("Recebido pedido de step")
        c.dia += 1
        schedule.every(5).seconds.do(c.update_lojas)

        while schedule.jobs:
            g_sleep(5)

        print("Pedido de step completo")

    return home()


@bottle.route('/')
def home():
    if c is None or not c.ready:
        return 'Loading...'

    return f'''
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    {util.get_css()}
    </style>
    </head>
    <body>
    {
    tabulate(
        [['ID do Produto', 'Classe', 'Quantidade', 'Status']] +
        [[
            p['pid'],
            p['classe'],
            p['qntd'],
            util.cor_estoque(p['classe'], p['qntd'])
        ] for p in list(c.estoque.db.values())],
        tablefmt='unsafehtml'
    )}
    <form method='post' action='/' class='step_button'>
        Dia: {c.dia}
        <input type='hidden' value='step' name='step'>
        <button type='submit'>Simular dia</button>
    </form>
    </body>
    </html>
    '''


if __name__ == '__main__':
    c = CentroDistribuicao()

    thr = threading.Thread(
        target=main,
        args=(c,),
        daemon=True
    )

    thr.start()

    bottle.run(host='0.0.0.0', port=5000, debug=True, server='gevent')

    thr.join(timeout=1)
