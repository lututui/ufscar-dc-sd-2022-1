import paho.mqtt.client as mqtt
import schedule as schedule

import util
from Estoque import Estoque

topic_namespace = 'ufscar/dc/sd/arthur'
topic = f'{topic_namespace}/centro-distribuicao'


class Fabrica:
    def __init__(self, fid):
        self.id = fid
        self.produtos = []


class Loja:
    def __init__(self, lid):
        self.id = lid


class CentroDistribuicao:
    def __init__(self):
        self.estoque = Estoque()
        self.fabricas: dict[str, Fabrica] = {}
        self.lojas: dict[str, Loja] = {}

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_subscribe = self.on_subscribe
        pass

    def heartbeat_lojas(self):
        lojas_conectadas = list(self.lojas.keys())

        if len(lojas_conectadas) >= 20:
            print('Recebido informação de todas as lojas')
            return schedule.CancelJob

        print('Requisitando heartbeat de lojas')

        self.lojas.clear()

        self.mqtt_client.publish(
            topic,
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
            topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.FABRICA, {})
        )

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        self.mqtt_client.subscribe(topic)

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
                    topic,
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

        print(f'Centro de distribuição recv unparsed msg {msg.payload.decode()}')

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print('Pedindo fábricas pela primeira vez')
        self.heartbeat_fabricas()

        schedule.every(15).seconds.do(self.heartbeat_fabricas)


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
