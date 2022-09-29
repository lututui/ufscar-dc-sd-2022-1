import random
import socket

import paho.mqtt.client as mqtt

import util
from Estoque import Estoque


class Loja:
    def __init__(self, lid):
        self.estoque = Estoque()
        self.id = lid
        self.pending_restoque = {}
        self.dia = 0

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        self.mqtt_client.subscribe(util.topic)

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
                    util.topic,
                    payload=util.build_msg(
                        util.MsgType.GET,
                        util.MsgTargetType.CENTRO_DISTRIBUICAO,
                        {'lid': self.id},
                    )
                )

            return

        if payload['type'] == util.MsgType.UPDATE:
            if payload['msg']['dia'] > self.dia:
                self.dia += 1

                print(f'Simulando vendas para o dia {self.dia}')

                v = self.simular_vendas()

                self.pending_restoque.update(v)
            else:
                v = self.pending_restoque

                print(f'Reenviando vendas do dia {self.dia}')

            self.mqtt_client.publish(
                util.topic,
                payload=util.build_msg(
                    util.MsgType.UPDATE,
                    util.MsgTargetType.CENTRO_DISTRIBUICAO,
                    {'lid': self.id, 'update': v, 'dia': self.dia}
                )
            )

            return

        if payload['type'] == util.MsgType.RESTOCK:
            for r_id in payload['msg'].keys():
                self.estoque.credito(r_id, payload['msg'][r_id])

                print(
                    f'Reestocou {r_id} x {payload["msg"][r_id]}: '
                    f'{self.estoque.db[r_id]["qntd"]}/{util.max_estoque(self.estoque.db[r_id]["classe"])}'
                )

            return

    def simular_vendas(self):
        relatorio_reestoque = {}

        tipos_comprados = random.choices(self.estoque.tipos_produtos, k=random.randrange(1, 5))

        for p in tipos_comprados:
            if self.estoque.db[p]['qntd'] <= 0:
                continue

            qntd_comprada = random.randrange(1, self.estoque.db[p]['qntd'])

            self.estoque.debito(p, qntd_comprada)

            print(f'Vendeu [Dia {self.dia}]: {p} x {qntd_comprada}: '
                  f'{self.estoque.db[p]["qntd"]}/{util.max_estoque(self.estoque.db[p]["classe"])}'
                  )

            qntd_reestoque = self.estoque.precisa_reestoque(p)

            if qntd_reestoque > 0:
                relatorio_reestoque[p] = qntd_reestoque

        return relatorio_reestoque


def main():
    loja = Loja(socket.gethostname())

    loja.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    loja.mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
