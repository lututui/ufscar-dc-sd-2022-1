import socket

import paho.mqtt.client as mqtt

import util
from Estoque import Estoque

topic_namespace = 'ufscar/dc/sd/arthur'
topic = f'{topic_namespace}/centro-distribuicao'


class Loja:
    def __init__(self, lid):
        self.estoque = Estoque()
        self.id = lid

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        self.mqtt_client.subscribe(topic)

    def on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        payload = util.decode_msg(msg.payload)

        if payload is None:
            return

        if payload['target'] != util.MsgTargetType.LOJA:
            return

        if payload['id'] != '*' and payload['id'] != self.id:
            return

        if payload['type'] == util.MsgType.GET:
            if self.id not in payload['msg']:
                print('Enviando heartbeat para centro de distribuição')

                self.mqtt_client.publish(
                    topic,
                    payload=util.build_msg(
                        util.MsgType.GET,
                        util.MsgTargetType.CENTRO_DISTRIBUICAO,
                        {'lid': self.id},
                    )
                )

            return


def main():
    loja = Loja(socket.gethostname())

    loja.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    loja.mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
