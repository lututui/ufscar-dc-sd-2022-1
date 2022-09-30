from gevent import monkey

monkey.patch_all()

import bottle as bottle
from paho.mqtt import client as mqtt
from tabulate import tabulate

from gevent.event import AsyncResult

import util


def wrap_html(head_elements: list[str], body_elements: list[str]):
    return f'''
    <!DOCTYPE html>
    <html>
    {wrap_head("".join(head_elements))}
    {wrap_body("".join(body_elements))}
    </html>
    '''


def wrap_head(to_wrap: str):
    return f'<head>{to_wrap}</head>'


def wrap_body(to_wrap: str):
    return f'<body>{to_wrap}</body>'


def get_log_css():
    return '''
    <style>
    div {
        display: inline;
    }
    </style>
    '''


def get_reload_tag():
    return '<meta http-equiv="refresh" content="5">'


def get_home_css():
    return '''
    <style>
    table {
        width: 75%;
        border: 1px solid #AAA;
    }
    td {
        border: 1px solid #CCC;
        text-align: center;

    }
    td > div {
        white-space: nowrap;
    }
    .box {
      display: inline-block;
      height: 20px;
      width: 20px;
      border: 1px solid black;
    }

    .red {
      background-color: red;
    }

    .green {
      background-color: green;
    }

    .yellow {
      background-color: yellow;
    }

    .button {
        background-color: #4CAF50; /* Green */
        border: none;
        color: white;
        padding: 15px 32px;
        text-align: center;
        text-decoration: none;
        display: inline-block;
        font-size: 16px;
    }

    .step_button {
        position:absolute;
        top: 30%;
        right: 10%;
    }
    </style>
    '''


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

    print(f'recv msg:{payload}')

    if 'data' in payload['msg']:
        print('Recv list')
        web_data.set(payload)
    elif 'log' in payload['msg']:
        print('Recv log')
        log_data.set(payload)


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


@bottle.route('/log')
def log():
    global log_data

    print('requesting log')

    __mqtt_client.publish(
        util.web_topic,
        payload=util.build_msg(
            util.MsgType.WEB,
            util.MsgTargetType.CENTRO_DISTRIBUICAO,
            msg_payload={'op': 'log'}
        )
    )

    info = log_data.get(block=True)
    info = info['msg']

    yield wrap_html([get_reload_tag(), get_log_css()], [info['log']])

    log_data = AsyncResult()


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

    # print(info)

    table = tabulate(
        [['ID do Produto', 'Classe', 'Quantidade', 'Status']] +
        [[
            p['pid'],
            p['classe'],
            f'{p["qntd"]}/{util.max_estoque(p["classe"], centro=True)} '
            f'(~{p["qntd"] / util.max_estoque(p["classe"], centro=True) * 100:.2f}%)',
            f'<div class="box {util.cor_estoque(p["classe"], p["qntd"], centro=True)}"></div>'
        ] for p in info['data']],
        tablefmt='unsafehtml'
    )
    step_button = f'''
    <form method="post" action="/" class="step_button">
        Dia: {info["dia"]}
        <input type="hidden" value="step" name="step">
        <button type="submit" class="button">Simular dia</button>
    </form>
    '''

    yield wrap_html([get_reload_tag(), get_home_css()], [table, step_button])

    web_data = AsyncResult()


__mqtt_client = mqtt.Client()
__mqtt_client.on_connect = __on_connect
__mqtt_client.on_message = __on_message

__mqtt_client.connect('broker.hivemq.com', 1883, 60)

web_data = AsyncResult()
log_data = AsyncResult()

__mqtt_client.loop_start()

bottle.run(host='0.0.0.0', port=5000, debug=True, server='gevent')

__mqtt_client.loop_stop()
