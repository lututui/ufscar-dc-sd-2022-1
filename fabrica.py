import socket
import warnings

import paho.mqtt.client as mqtt

import util

topic_namespace = 'ufscar/dc/sd/arthur'
topic = f'{topic_namespace}/centro-distribuicao'


class Fabrica:
    def __init__(self, fid: str):
        self.produtos: list[int] = []
        self.id = fid

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

        if payload['target'] != util.MsgTargetType.FABRICA:
            return

        if payload['id'] != '*' and payload['id'] != self.id:
            return

        print(f'fabrica {self.id} recv msg {msg.payload.decode()}')

        if payload['type'] == util.MsgType.GET:
            if self.id not in payload['msg']:
                print('Enviando heartbeat para centro de distribuição')

                self.mqtt_client.publish(
                    topic,
                    payload=util.build_msg(
                        util.MsgType.GET,
                        util.MsgTargetType.CENTRO_DISTRIBUICAO,
                        {'fid': self.id},
                    )
                )

            return

        if payload['type'] == util.MsgType.SET:
            self.produtos = payload['msg']['produtos']

            if len(self.produtos) != 3:
                warnings.warn(f'[{self.id}] recebeu lista de produtos de tamanho inesperado: {len(self.produtos)}')

            return

        warnings.warn(f'[{self.id}] msg não reconhecida: {msg.payload.decode()}')


def main():
    fab = Fabrica(fid=socket.gethostname())

    fab.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    fab.mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
