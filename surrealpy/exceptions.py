import re
from typing import Any, Union
import typing
from surrealpy import utils

SURREALQL_SYNTAX_REGEX = re.compile(r"line (\d+) at character (\d+)")


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


    def __init__(self, message: Union[dict[str, Any], str]):
        if type(message) == str:
            self.message = "Uncaught: %s" % message
        else:
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


class SurrealDBCliError(SurrealError):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message


class SurrealQLSyntaxError(SurrealError):
    """Exception raised for errors in the head of the statement.

    Parameters
    ----------
    message : str
        explanation of the error
    query : str
        the query that caused the error
    line : typing.Optional[int] (optional)
        the line that caused the error, by default None
    index : typing.Optional[int] (optional)
        the index that caused the error, by default None
    """

    def __init__(
        self,
        message: str,
        query: str,
        *,
        line: typing.Optional[int] = None,
        index: typing.Optional[int] = None,
    ):
        self.message = message
        self.query = query
        match = SURREALQL_SYNTAX_REGEX.search(message)
        
        self.line = line or int(match.group(1)) if match else None
        self.index = index or int(match.group(2)) if match else None

    def __str__(self):

        if self.line is not None and self.index is not None:
            # not finished yet but it works if line and index are given as parameters
            queryLines: typing.List[str] = self.query.split("\n")
            query: str = queryLines[self.line - 1]
            
            lastAt = len(query)
            errorString = query[self.index : lastAt ]
            # replace original query's self.index to lastAt with errorString
            queryLines[self.line - 1] = (
                query[: self.index] + utils.colored(errorString, "red") + query[lastAt:]
            )
            query = "\n".join(queryLines)
            queryLines[
                self.line - 1
            ] = f"{queryLines[self.line-1]}\n{' '*self.index}{utils.colored('^'*len(errorString),'cyan')}"
            query = "\n".join(queryLines)
            
            return (
                f"{self.message}:\n{query}"
            )

        else:
            return super().__str__()

class ValidationError(SurrealError):
    """Exception raised for errors in the head of the statement.
    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message