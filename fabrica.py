import socket

import paho.mqtt.client as mqtt

import util


class Fabrica:
    def __init__(self, fid: str):
        # Lista de produtos produzidos por esta fábrica
        # Inicialmente vazia, definida quando o centro de distribuição identificar esta fábrica
        self.produtos: list[int] = []

        self.id = fid

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # Callback de conexão no Broker
    def on_connect(self, _, __, ___, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        # Inscreve-se no tópico principal, para comunicar-se com o centro de distribuição
        self.mqtt_client.subscribe(util.main_topic)

    # Callback de recebimento de mensagem
    def on_message(self, _, __, msg: mqtt.MQTTMessage):
        payload = util.decode_msg(msg.payload)

        # Descarta mensagem vazia ou não-decodificada
        if payload is None:
            return

        # Descarta mensagens que não são para fábricas
        if payload['target'] != util.MsgTargetType.FABRICA:
            return

        # Descarta mensagens que não sejam para esta fábrica
        if payload['id'] != '*' and payload['id'] != self.id:
            return

        print(f'fabrica {self.id} recv msg {msg.payload.decode()}')

        # Recebeu mensagem de pedido de identificação
        if payload['type'] == util.MsgType.GET:
            print('Enviando heartbeat para centro de distribuição')

            # Resposta com o ID desta fábrica
            self.mqtt_client.publish(
                util.main_topic,
                payload=util.build_msg(
                    util.MsgType.GET,
                    util.MsgTargetType.CENTRO_DISTRIBUICAO,
                    {'fid': self.id},
                )
            )

            return

        # Recebeu mensagem de definição de produtos fabricados
        if payload['type'] == util.MsgType.SET:
            self.produtos = payload['msg']['produtos']

            if len(self.produtos) != 3:
                print(f'[{self.id}] recebeu lista de produtos de tamanho inesperado: {len(self.produtos)}')

            return

        # Recebeu mensagem de fabricação do centro de distribuição
        if payload['type'] == util.MsgType.UPDATE:
            # Esta fábrica não fabrica este produto
            if payload["msg"]["pid"] not in self.produtos:
                raise Exception(f'Centro de distribuição pediu {payload["msg"]["pid"]} '
                                f'mas não é fabricado aqui: {self.produtos}')

            print(f'Centro de distribuição pediu {payload["msg"]["pid"]} x {payload["msg"]["qntd"]}')

            # Envia reposição para o centro de distribuição
            self.mqtt_client.publish(
                util.main_topic,
                payload=util.build_msg(
                    util.MsgType.RESTOCK,
                    util.MsgTargetType.CENTRO_DISTRIBUICAO,
                    msg_payload={'fid': self.id, 'pid': payload['msg']['pid'], 'qntd': payload['msg']['qntd']}
                )
            )
            return

        # Mensagem desconhecida
        print(f'[{self.id}] msg não reconhecida: {msg.payload.decode()}')


def main():
    fab = Fabrica(fid=socket.gethostname())

    # Conecta-se ao broker
    fab.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    fab.mqtt_client.loop_forever()


if __name__ == '__main__':
    main()
