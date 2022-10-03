import paho.mqtt.client as mqtt
import schedule as schedule

import util
from Estoque import Estoque


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

        self.dia = 0
        self.buffer_update = {}

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_subscribe = self.on_subscribe

    # Identifica todas as lojas
    def heartbeat_lojas(self):
        lojas_conectadas = list(self.lojas.keys())

        # Todas as lojas responderam o pedido de identificação
        if len(lojas_conectadas) >= util.qntd_lojas:
            print('Recebido informação de todas as lojas')

            # Cancela schedule para pedir identificação das lojas: todas já foram identificadas
            return schedule.CancelJob

        print('Requisitando heartbeat de lojas')

        # Timeout no pedido de identificação
        self.lojas.clear()

        # Enviar pedido de identificação para as LOJAS
        self.mqtt_client.publish(
            util.main_topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.LOJA, {})
        )

    # Identifica todas as fábricas
    def heartbeat_fabricas(self):
        fabricas_conectadas = list(self.fabricas.keys())

        # Todas as fábricas responderam o pedido de identificação
        if len(fabricas_conectadas) >= util.qntd_fabricas:
            print('Recebido informação de todas as fabricas')

            # Inicia pedido de identificação das lojas
            schedule.every(util.timeout_lojas).seconds.do(self.heartbeat_lojas)

            # Cancela schedule para pedir identificação das fábricas: todas já foram identificadas
            return schedule.CancelJob

        print('Requisitando heartbeat de fábricas')

        # print(self.fabricas)

        # Timeout no pedido de identificação
        self.fabricas.clear()

        # Enviar pedido de identificação para as FÁBRICAS
        self.mqtt_client.publish(
            util.main_topic,
            payload=util.build_msg(util.MsgType.GET, util.MsgTargetType.FABRICA, {})
        )

    # Callback de conexão no Broker
    def on_connect(self, _, __, ___, rc):
        if rc != 0:
            raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

        # Pede subscribe no tópico principal
        self.mqtt_client.subscribe(util.main_topic)

    # Recebe e gerencia estoque de fábricas
    def update_lojas(self):
        # Recebeu resposta de update de estoque de todas as lojas
        if len(self.buffer_update) == util.qntd_lojas:
            print('Recebeu todos os updates')

            # Para cada loja (por id da loja)...
            for lid in self.buffer_update:
                # print(f'{lid} [{type(lid)}] = {self.buffer_update[lid]}')

                # Pedido de produtos enviado pela loja
                pedido_loja = self.buffer_update[lid]

                # Lista de pedidos que o centro de distribuição não pode cumprir no momento
                impossivel_completar = []

                # Para cada produto no pedido da loja (por id do produto)...
                for produto in pedido_loja:
                    # Quantidade do produto pedido pela loja
                    qntd = pedido_loja[produto]

                    print(f'Loja {lid} pediu reestoque de {produto} x {qntd} no dia {self.dia}')

                    # Tenta executar débito no estoque do centro de distribuição,
                    # para enviar para a loja que fez o pedido
                    if not self.estoque.debito(produto, qntd):
                        # Débito falhou: não há estoque para atender o pedido
                        impossivel_completar.append(produto)

                print(f'Processou pedido da loja {lid}')

                # Retém pedidos não completados
                for imp in impossivel_completar:
                    del pedido_loja[imp]

                # Envia reabastecimento possível
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

                    # Verificar estoque da loja, por estoques abaixo de 25%
                    self.verificar_inventario()

            self.buffer_update.clear()

            # Cancela schedule de atualização de estoque
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

            # Estoque do produto abaixo de 25%
            if util.cor_estoque(classe, qntd_atual, centro=True) != 'red':
                continue

            # Quanto produto o centro de distribuição precisa para preencher seu estoque
            qntd_necessaria = util.max_estoque(classe, centro=True) - qntd_atual

            print(f'Pedindo reestoque de {pid} x {qntd_necessaria}')

            # Encontrar fábrica que fabrique o produto necessário
            for fid, fab in self.fabricas.items():
                print(f'Fabrica {fid}: {fab.produtos}')

                if pid not in fab.produtos:
                    continue

                # Encontrou fábrica
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

            # Nenhuma fábrica conhecida fabrica o produto
            if not sucesso:
                raise Exception(f'Não foi encontrada fábrica que fabrique {pid}\n'
                                f'{[f.produtos for f in self.fabricas.values()]}')

    # Lida com mensagens enviadas pelo visualizador
    def web_message(self, payload):
        # print('recv web msg')

        # Descarta mensagens com tipo incorreto
        if payload['type'] != util.MsgType.WEB:
            return

        # Visualizador pediu para listar o estoque
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

        # Visualizador pediu para simular dia de vendas
        if payload['msg']['op'] == 'step':
            print('Recebeu mensagem de step')
            self.dia += 1
            schedule.every(util.timeout_lojas).seconds.do(self.update_lojas)
            return

        # Visualizador pediu log de operações do estoque do centro de distribuição
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

        # Mensagem inesperada no canal
        print(f'Centro de distribuição recv unparsed WEB msg {payload}')

        return

    # Lida com mensagens enviadas por fábricas e lojas
    def main_message(self, payload):
        # Mensagem de identificação
        if payload['type'] == util.MsgType.GET:
            # Fábrica
            if 'fid' in payload['msg']:
                # print(payload['msg'])
                uuid = payload['msg']['fid']

                self.fabricas[uuid] = Fabrica(uuid)

                f_idx = len(self.fabricas) - 1

                # Define quais produtos a fábrica se identificando deve produzir
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

            # Loja
            if 'lid' in payload['msg']:
                uuid = payload['msg']['lid']
                self.lojas[uuid] = Loja(uuid)

                print(f'Lojas identificadas: {len(self.lojas)}/{util.qntd_lojas}')
                return

            return

        # Mensagem de update de estoque das lojas
        if payload['type'] == util.MsgType.UPDATE:
            # Descarta mensagens de dias incorretos (reenvios atrasados de dias anteriores, por exemplo)
            if payload['msg']['dia'] != self.dia:
                return

            # Descarta mensagens de lojas que já estão na fila de processamento
            if payload['msg']['lid'] in self.buffer_update:
                return

            # Coloca a mensagem na fila de processamento
            self.buffer_update[payload['msg']['lid']] = payload['msg']['update']
            print(f'[Dia {self.dia}] Recebeu update de loja: {len(self.buffer_update)}/{util.qntd_lojas}')
            return

        # Mensagem de reabastecimento de produtos (enviados pelas fábricas)
        if payload['type'] == util.MsgType.RESTOCK:
            pid = payload['msg']['pid']
            qntd = payload['msg']['qntd']

            print(f'Recebeu reestoque de fábrica {payload["msg"]["fid"]}: {pid} x {qntd}')
            self.estoque.credito(pid, qntd)
            return

        print(f'Centro de distribuição recv unparsed msg {payload}')

    # Callback para lidar com mensagens recebidas
    def on_message(self, _, __, msg: mqtt.MQTTMessage):
        # print(msg.topic)
        payload = util.decode_msg(msg.payload)

        # Descarta mensagens completamente vazias ou não decodificadas
        if payload is None:
            return

        # Descarta mensagens não destinadas ao centro de distribuição
        if payload['target'] != util.MsgTargetType.CENTRO_DISTRIBUICAO:
            return

        # Distribui mensagens de acordo com o tópico
        if msg.topic == util.web_topic:
            self.web_message(payload)
        elif msg.topic == util.main_topic:
            self.main_message(payload)
        else:
            raise Exception(f'Unknown topic: {msg.topic}')

    # Callback para iniciar processo de identificação, uma vez que o centro se conectou ao tópico principal
    def on_subscribe(self, _, __, ___, ____):
        # Agenda identificação de fábricas
        schedule.every(util.timeout_fabricas).seconds.do(self.heartbeat_fabricas)

        # Cancela este callback
        self.mqtt_client.on_subscribe = None

        # Se inscreve no tópico do visualizador
        self.mqtt_client.subscribe(util.web_topic)


def main():
    centro = CentroDistribuicao()

    # Conecta-se ao broker
    centro.mqtt_client.connect('broker.hivemq.com', 1883, 60)
    centro.mqtt_client.loop_start()

    try:
        while True:
            # Executa tarefas pendentes
            # github.com/dbader/schedule
            schedule.run_pending()
    except KeyboardInterrupt:
        pass

    centro.mqtt_client.loop_stop()


if __name__ == '__main__':
    main()
