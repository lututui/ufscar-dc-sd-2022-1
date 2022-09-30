import json
import math
import warnings
from enum import Enum

topic_namespace = 'ufscar/dc/sd/arthur'
main_topic = f'{topic_namespace}/centro-distribuicao'
web_topic = f'{topic_namespace}/web'

qntd_lojas = 2
qntd_fabricas = 2
qntd_produtos = 3 * qntd_fabricas

timeout_fabricas = math.ceil(0.15 * qntd_fabricas)
timeout_lojas = math.ceil(0.15 * qntd_lojas)


class __CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Enum):
            return o.value

        return super().default(o)


class __CustomDecoder(json.JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=self.object_hook)

    def object_hook(self, s):
        if isinstance(s, str):
            try:
                return int(s)
            except ValueError:
                return s

        if isinstance(s, dict):
            return {self.object_hook(k): self.object_hook(v) for k, v in s.items()}

        if isinstance(s, list):
            return [self.object_hook(v) for v in s]

        return s


class MsgType(Enum):
    GET = 1
    SET = 2
    UPDATE = 3
    RESTOCK = 4
    WEB = 5

    def __eq__(self, o: object) -> bool:
        if isinstance(o, int):
            return self.value == o
        return super().__eq__(o)


class MsgTargetType(Enum):
    FABRICA = 1
    CENTRO_DISTRIBUICAO = 2
    LOJA = 3
    WEB = 4

    def __eq__(self, o: object) -> bool:
        if isinstance(o, int):
            return self.value == o
        return super().__eq__(o)


def build_msg(msg_type: MsgType, msg_target_type: MsgTargetType, msg_payload, msg_target_id: str = '*') -> str:
    return json.dumps(
        {'type': msg_type, 'target': msg_target_type, 'id': msg_target_id, 'msg': msg_payload},
        cls=__CustomEncoder
    )


def decode_msg(payload: bytes):
    try:
        decoded_payload = json.loads(payload, cls=__CustomDecoder)

        return decoded_payload
    except json.JSONDecodeError as err:
        warnings.warn(f'failed to decode incoming payload: {payload.decode()}\nerror: {err.msg}')
        return None


def max_estoque(classe: str, centro: bool = False) -> int:
    mult = 1

    if centro:
        mult *= qntd_lojas

    if classe == 'A':
        return mult * 100

    if classe == 'B':
        return mult * 60

    if classe == 'C':
        return mult * 20

    raise Exception(f'Classe de produto desconhecida: {classe}')


def cor_estoque(classe: str, qntd: int, centro: bool = False) -> str:
    percentual = qntd * 100 / max_estoque(classe, centro=centro)

    if percentual > 50:
        return 'green'

    if percentual > 25:
        return 'yellow'

    return 'red'


def wrap_color(color: str, text: str):
    return f'<div style="color:{color}">{text}</div>'
