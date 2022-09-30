from gevent import monkey

monkey.patch_all()

import bottle as bottle
from paho.mqtt import client as mqtt
from tabulate import tabulate

from gevent.event import AsyncResult

import util


def __on_connect(mqtt_client, userdata, flags, rc):
    if rc != 0:
        raise Exception("Conexão com broker falhou: " + mqtt.connack_string(rc))

    mqtt_client.subscribe(util.web_topic)


def __on_message(mqtt_client, userdata, msg: mqtt.MQTTMessage):
    global web_data

    payload = util.decode_msg(msg.payload)

    if payload is None:
        return

    if payload['target'] != util.MsgTargetType.WEB:
        return

    print('recv msg')

    web_data.set(payload)


@bottle.route('/', method='POST')
def home_post():
    step = bottle.request.forms.get('step') == 'step'

    if step:
        print("Recebido pedido de step")

        __mqtt_client.publish(
            util.web_topic,
            payload=util.build_msg(
                util.MsgType.WEB,
                util.MsgTargetType.CENTRO_DISTRIBUICAO,
                msg_payload={'op': 'step'}
            )
        )

    return home()


@bottle.route('/')
def home():
    global web_data

    print('requesting list')

    __mqtt_client.publish(
        util.web_topic,
        payload=util.build_msg(
            util.MsgType.WEB,
            util.MsgTargetType.CENTRO_DISTRIBUICAO,
            msg_payload={'op': 'list'}
        )
    )

    info = web_data.get(block=True)
    info = info['msg']

    print(info)

    yield f'''
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
            f'{p["qntd"]}/{util.max_estoque(p["classe"], centro=True)} '
            f'(~{p["qntd"]/util.max_estoque(p["classe"], centro=True)*100:.2f}%)',
            f'<div class="box {util.cor_estoque(p["classe"], p["qntd"], centro=True)}"></div>'
        ] for p in info['data']],
        tablefmt='unsafehtml'
    )}
    <form method='post' action='/' class='step_button'>
        Dia: {info['dia']}
        <input type='hidden' value='step' name='step'>
        <button type='submit' class='button'>Simular dia</button>
    </form>
    </body>
    </html>
    '''

    web_data = AsyncResult()


__mqtt_client = mqtt.Client()
__mqtt_client.on_connect = __on_connect
__mqtt_client.on_message = __on_message

__mqtt_client.connect('broker.hivemq.com', 1883, 60)

web_data = AsyncResult()

__mqtt_client.loop_start()

bottle.run(host='0.0.0.0', port=5000, debug=True, server='gevent')

__mqtt_client.loop_stop()
