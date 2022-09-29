import threading

import bottle as bottle
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


def home_css():
    return '''
    .tabs {
      position: relative;   
      min-height: 200px; /* This part sucks */
      clear: both;
      margin: 25px 0;
    }
    .tab {
      float: left;
    }
    .tab label {
      background: #eee; 
      padding: 10px; 
      border: 1px solid #ccc; 
      margin-left: -1px; 
      position: relative;
      left: 1px; 
    }
    .tab [type=radio] {
      display: none;   
    }
    .content {
      position: absolute;
      top: 28px;
      left: 0;
      background: white;
      right: 0;
      bottom: 0;
      padding: 20px;
      border: 1px solid #ccc; 
    }
    [type=radio]:checked ~ label {
      background: white;
      border-bottom: 1px solid white;
      z-index: 2;
    }
    [type=radio]:checked ~ label ~ .content {
      z-index: 1;
    }
    '''


@bottle.route('/')
def home():
    if c is None:
        return 'Loading...'

    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <style>{home_css()}</style>
    </head>
    <body>
    <div class="tabs">
        <div class="tab">
            <input type="radio" id="tab-1" name="tab-group-1" checked>
            <label for="tab-1">Tab One</label>
            <div class="content">
                stuff
            </div>
        </div>
        <div class="tab">
            <input type="radio" id="tab-2" name="tab-group-1">
            <label for="tab-2">Tab Two</label>
            <div class="content">
                stuff
            </div>
        </div>
        <div class="tab">
            <input type="radio" id="tab-3" name="tab-group-1">
            <label for="tab-3">Tab Three</label>
            <div class="content">
                stuff
            </div>
        </div>
    </div>
    </body>
    </home>
    '''


if __name__ == '__main__':
    c = CentroDistribuicao()

    thr = threading.Thread(target=bottle.run, kwargs={'host': '0.0.0.0', 'port': 5000, 'debug': True})

    thr.start()

    main(c)

    thr.join()
