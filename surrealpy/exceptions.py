class BaseException(Exception):
    """Base class for all exceptions in this module."""

    pass


class ConnectionError(BaseException):
    """Exception raised for errors in the connection.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class SurrealError(BaseException):
    """Exception raised for errors in the surreal db.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class SurrealStatementHeadError(BaseException):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class EmptyStatementError(BaseException):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
