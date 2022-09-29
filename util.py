import json
import warnings
from enum import Enum
from typing import Any

topic_namespace = 'ufscar/dc/sd/arthur'
topic = f'{topic_namespace}/centro-distribuicao'


class ProjectCustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value

        return super().default(o)


class MsgType(Enum):
    GET = 1
    SET = 2
    UPDATE = 3
    RESTOCK = 4

    def __eq__(self, o: object) -> bool:
        if isinstance(o, int):
            return self.value == o
        return super().__eq__(o)


class MsgTargetType(Enum):
    FABRICA = 1
    CENTRO_DISTRIBUICAO = 2
    LOJA = 3

    def __eq__(self, o: object) -> bool:
        if isinstance(o, int):
            return self.value == o
        return super().__eq__(o)


def build_msg(msg_type: MsgType, msg_target_type: MsgTargetType, msg_payload: Any, msg_target_id: str = '*') -> str:
    return json.dumps(
        {'type': msg_type, 'target': msg_target_type, 'id': msg_target_id, 'msg': msg_payload},
        cls=ProjectCustomEncoder
    )


def decode_msg(payload: bytes):
    try:
        decoded_payload = json.loads(payload)

        return decoded_payload
    except json.JSONDecodeError as err:
        warnings.warn(f'failed to decode incoming payload: {payload.decode()}\nerror: {err.msg}')
        return None


def max_estoque(classe: str) -> int:
    if classe == 'A':
        return 100

    if classe == 'B':
        return 60

    if classe == 'C':
        return 20

    raise Exception(f'Classe de produto desconhecida: {classe}')


def cor_estoque(classe: str, qntd: int) -> str:
    percentual = qntd * 100 / max_estoque(classe)

    if percentual > 50:
        color = 'green'
    elif percentual > 25:
        color = 'yellow'
    else:
        color = 'red'

    return f'<div class="box {color}"></div>'


def get_css():
    return '''
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
    
    .blue {
      background-color: blue;
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
    '''