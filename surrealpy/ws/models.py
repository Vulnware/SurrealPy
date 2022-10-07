import dataclasses
from typing import Any, Optional, Union

__all__ = ("SurrealRequest",)


@dataclasses.dataclass(frozen=True)
class LoginParams:
    """
    A class used to represent SurrealDB login parameters.
    ...

    Attributes
    ----------
    username: str
        The username of the user. alias of field is user
    password: str
        The password of the user. alias of field is pass

    Methods
    -------
    to_dict() -> dict[str, Any]
        Convert the SurrealDB login parameters to a dict with alias as keys
    """

    username: str = dataclasses.field(metadata={"alias": "user"})
    password: str = dataclasses.field(metadata={"alias": "pass"})

    def __post_init__(self):
        if not self.username:
            raise ValueError("username is required")
        if not self.password:
            raise ValueError("password is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            dataclasses.fields(self)[i].metadata["alias"]: getattr(
                self, dataclasses.fields(self)[i].name
            )
            for i in range(len(dataclasses.fields(self)))
        }


@dataclasses.dataclass(frozen=True)
class SurrealRequest:
    id: str
    method: str
    params: tuple[Any]


@dataclasses.dataclass(frozen=True)
class SurrealResponse:
    id: str
    results: Union[dict, list[Any]]
