import paho.mqtt.client as mqtt
import schedule as schedule

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
        self.estoque = Estoque(centro=True, log=True)
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

        if len(lojas_conectadas) >= util.qntd_lojas:
            print('Recebido informação de todas as lojas')

            return schedule.CancelJob

        print('Requisitando heartbeat de lojas')

        self.lojas.clear()

        self.mqtt_client.publish(
            util.main_topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.LOJA, {})
        )

    def heartbeat_fabricas(self):
        fabricas_conectadas = list(self.fabricas.keys())

        if len(fabricas_conectadas) >= util.qntd_fabricas:
            print('Recebido informação de todas as fabricas')
            print('Pedindo lojas pela primeira vez')
            self.heartbeat_lojas()

            schedule.every(util.timeout_lojas).seconds.do(self.heartbeat_lojas)
            return schedule.CancelJob

        print('Requisitando heartbeat de fábricas')

        # print(self.fabricas)

        self.fabricas.clear()

        self.mqtt_client.publish(
            util.main_topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.FABRICA, {})
        )

    def on_connect(self, _, __, ___, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        self.mqtt_client.subscribe(util.main_topic)

    def update_lojas(self):
        if len(self.buffer_update) == util.qntd_lojas:
            print('Recebeu todos os updates')

            for lid in self.buffer_update:
                # print(f'{lid} [{type(lid)}] = {self.buffer_update[lid]}')

                pedido_loja = self.buffer_update[lid]

                impossivel_completar = []

                for produto in pedido_loja:
                    qntd = pedido_loja[produto]

                    print(f'Loja {lid} pediu reestoque de {produto} x {qntd} no dia {self.dia}')

                    if not self.estoque.debito(produto, qntd):
                        impossivel_completar.append(produto)

                print(f'Processou pedido da loja {lid}')

                for imp in impossivel_completar:
                    del pedido_loja[imp]

                if len(pedido_loja) > 0:
                    print(f'Enviando reestoque para loja {lid}')

                    self.mqtt_client.publish(
                        util.main_topic,
                        payload=util.build_msg(
                            util.MsgType.RESTOCK,
                            util.MsgTargetType.LOJA,
                            pedido_loja,
                            msg_target_id=lid
                        )
                    )

                    self.verificar_inventario()

            self.buffer_update.clear()

            return schedule.CancelJob

        print(f'[Dia {self.dia}] Enviando pedido de update para todas as lojas')

        self.mqtt_client.publish(
            util.main_topic,
            payload=util.build_msg(
                util.MsgType.UPDATE,
                util.MsgTargetType.LOJA,
                msg_payload={'dia': self.dia}
            )
        )

    def verificar_inventario(self):
        print('Verificando estoque...')

        for pid, prod in self.estoque.db.items():
            sucesso = False

            classe = prod['classe']
            qntd_atual = prod['qntd']

            print(f'{pid} x {qntd_atual} : {util.cor_estoque(classe, qntd_atual, centro=True)}')

            if util.cor_estoque(classe, qntd_atual, centro=True) != 'red':
                continue

            qntd_necessaria = util.max_estoque(classe, centro=True) - qntd_atual

            print(f'Pedindo reestoque de {pid} x {qntd_necessaria}')

            for fid, fab in self.fabricas.items():
                print(f'Fabrica {fid}: {fab.produtos}')

                if pid not in fab.produtos:
                    continue

                print(f'Pedindo reestoque de {pid} x {qntd_necessaria} para fábrica {fid}')

                self.mqtt_client.publish(
                    util.main_topic,
                    payload=util.build_msg(
                        util.MsgType.UPDATE,
                        util.MsgTargetType.FABRICA,
                        msg_payload={'pid': pid, 'qntd': qntd_necessaria},
                        msg_target_id=fid
                    )
                )

                sucesso = True
                break

            if not sucesso:
                raise Exception(f'Não foi encontrada fábrica que fabrique {pid}\n'
                                f'{[f.produtos for f in self.fabricas.values()]}')

    def web_message(self, payload):
        # print('recv web msg')

        if payload['type'] != util.MsgType.WEB:
            return

        if payload['msg']['op'] == 'list':
            # print('recv list msg')
            self.mqtt_client.publish(
                util.web_topic,
                payload=util.build_msg(
                    util.MsgType.WEB,
                    util.MsgTargetType.WEB,
                    msg_payload={'data': list(self.estoque.db.values()), 'dia': self.dia}
                )
            )
            return

        if payload['msg']['op'] == 'step':
            print('Recebeu mensagem de step')
            self.dia += 1
            schedule.every(util.timeout_lojas).seconds.do(self.update_lojas)
            return

        if payload['msg']['op'] == 'log':
            print('Recebeu mensagem de log')
            self.mqtt_client.publish(
                util.web_topic,
                payload=util.build_msg(
                    util.MsgType.WEB,
                    util.MsgTargetType.WEB,
                    msg_payload={'log': '<br>'.join(self.estoque.log_data)}
                )
            )
            return

        print(f'Centro de distribuição recv unparsed WEB msg {payload}')

        return

    def main_message(self, payload):
        if payload['type'] == util.MsgType.GET:
            # Fábrica
            if 'fid' in payload['msg']:
                # print(payload['msg'])
                uuid = payload['msg']['fid']

                self.fabricas[uuid] = Fabrica(uuid)

                f_idx = len(self.fabricas) - 1

                self.fabricas[uuid].produtos = self.estoque.tipos_produtos[3 * f_idx:3 * (f_idx + 1)]

                self.mqtt_client.publish(
                    util.main_topic,
                    payload=util.build_msg(
                        util.MsgType.SET,
                        util.MsgTargetType.FABRICA,
                        {"produtos": self.fabricas[uuid].produtos},
                        msg_target_id=uuid
                    )
                )

                print(f'Fábricas identificadas: {len(self.fabricas)}/{util.qntd_fabricas}')
                return

            if 'lid' in payload['msg']:
                uuid = payload['msg']['lid']
                self.lojas[uuid] = Loja(uuid)

                print(f'Lojas identificadas: {len(self.lojas)}/{util.qntd_lojas}')
                return

            return

        if payload['type'] == util.MsgType.UPDATE:
            if payload['msg']['dia'] != self.dia:
                return

            if payload['msg']['lid'] in self.buffer_update:
                return

            self.buffer_update[payload['msg']['lid']] = payload['msg']['update']
            print(f'[Dia {self.dia}] Recebeu update de loja: {len(self.buffer_update)}/{util.qntd_lojas}')
            return

        if payload['type'] == util.MsgType.RESTOCK:
            pid = payload['msg']['pid']
            qntd = payload['msg']['qntd']

            print(f'Recebeu reestoque de fábrica {payload["msg"]["fid"]}: {pid} x {qntd}')
            self.estoque.credito(pid, qntd)
            return

        print(f'Centro de distribuição recv unparsed msg {payload}')

    def on_message(self, _, __, msg: mqtt.MQTTMessage):
        # print(msg.topic)
        payload = util.decode_msg(msg.payload)

        if payload is None:
            return

        if payload['target'] != util.MsgTargetType.CENTRO_DISTRIBUICAO:
            return

        if msg.topic == util.web_topic:
            self.web_message(payload)
        elif msg.topic == util.main_topic:
            self.main_message(payload)
        else:
            raise Exception(f'Unknown topic: {msg.topic}')

    def on_subscribe(self, _, __, ___, ____):
        schedule.every(util.timeout_fabricas).seconds.do(self.heartbeat_fabricas)

        self.mqtt_client.on_subscribe = None
        self.mqtt_client.subscribe(util.web_topic)


def main():
    centro = CentroDistribuicao()

    centro.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    centro.mqtt_client.loop_start()

    try:
        while True:
            schedule.run_pending()
    except KeyboardInterrupt:
        pass

    centro.mqtt_client.loop_stop()


if __name__ == '__main__':
    main()
