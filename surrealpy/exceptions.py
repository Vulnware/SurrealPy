from typing import Any


class BaseException(Exception):
    """Base class for all exceptions in this module."""

    def __str__(self) -> str:
        return self.message


class SurrealError(BaseException):
    """Exception raised for errors in the surreal db.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class ConnectionError(SurrealError):
    """Exception raised for errors in the connection.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class WebSocketError(SurrealError):
    """Exception raised for errors in the websocket.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: dict[str, Any]):
        self.message = f"{message['message']} ({message['code']})"


class SurrealStatementHeadError(SurrealError):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class EmptyStatementError(SurrealError):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
