import json
import warnings
from enum import Enum
from typing import Any


class ProjectCustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value

        return super().default(o)


class MsgType(Enum):
    GET = 1
    SET = 2

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
