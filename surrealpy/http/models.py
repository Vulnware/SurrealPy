from typing import Optional
import dataclasses
from surrealpy import utils


@dataclasses.dataclass(frozen=True)
class Result:
    time: str
    status: str
    result: list[Optional[dict]]


@dataclasses.dataclass(frozen=True)
class SurrealResponse:
    result: list[Result]


def to_json(obj):
    return utils.json_dumps(dataclasses.asdict(obj))


def to_dict(obj):
    return dataclasses.asdict(obj)


def from_json(json, cls=SurrealResponse):
    return cls(**utils.json_loads(json))


def from_dict(dict, cls=SurrealResponse):
    return cls(**dict)
