from types import TracebackType
from typing import Any, Optional, Union
import logging
import urllib.parse
from surrealpy.http.models import SurrealResponse
from surrealpy.exceptions import ConnectionError, SurrealError
from surrealpy.utils import json_dumps, json_loads
import httpx

__all__ = ("Client", "AsyncClient")


def base_url(url, with_path=False):
    # Get From https://stackoverflow.com/questions/35616434/how-can-i-get-the-base-of-a-url-in-python
    parsed = urllib.parse.urlparse(url)
    path = "/".join(parsed.path.split("/")[:-1]) if with_path else ""
    parsed = parsed._replace(path=path)
    parsed = parsed._replace(params="")
    parsed = parsed._replace(query="")
    parsed = parsed._replace(fragment="")
    return parsed.geturl()


# create logger object
logger = logging.getLogger("surrealpy.http")
logger.setLevel(logging.DEBUG)
logger.propagate = True
logger.debug("imported")


class AsyncClient:
    """
    This class is a wrapper for the SurrealDB HTTP API.
    ...
    Parameters
    ----------
    uri : str
        The URI of the SurrealDB server.
    namespace : str
        The namespace of the database.
    database : str
        The name of the database.
    username : str
        The username of the database.
    password : str
        The password of the database.

    Attributes
    ----------
    uri : str
        the uri of the database
    namespace : str
        the namespace of the database
    database : str
        the database name
    username : str
        the username of the database
    password : str
        the password of the database

    """

    def __init__(
        self, uri: str, namespace: str, database: str, *, username: str, password: str
    ) -> None:
        """
        Constructor of the class.

        Parameters
        ----------
        uri : str
            The URI of the SurrealDB server.
        namespace : str
            The namespace of the database.
        database : str
            The name of the database.
        username : str
            The username of the database.
        password : str
            The password of the database.
        """
        self.__session = httpx.AsyncClient(
            auth=httpx.BasicAuth(username=username, password=password)
        )
        self.__session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "NS": namespace,
                "DB": database,
            }
        )
        self.__uri = uri
        self.__baseurl = base_url(uri)
        self.__ns = namespace
        self.__db = database

    @property
    def ns(self) -> str:
        """
        The namespace of the database.

        Returns
        -------
        str
            the namespace of the database
        """
        return self.__ns

    @property
    def db(self) -> Union[dict[str, Any], list[Any]]:
        """
        The databases in the namespace.

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the databases in the namespace as a list of dictionaries or a dictionary
        """
        return self.__db

    async def tables(self) -> Union[dict[str, Any], list[Any]]:
        """
        The tables in the database.

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the tables in the database as a list of dictionaries or a dictionary
        """
        # retrieve all tables
        data = await self.raw_query("INFO FOR DB;")
        for i in data[0]["result"]["tb"].keys():
            yield i

    @property
    def uri(self) -> str:
        """
        The URI of the SurrealDB server.

        Returns
        -------
        str
            the URI of the SurrealDB server as a string
        """
        return self.__uri

    async def __check(self) -> None:
        """
        Check if the connection is valid. If not, raise an exception.

        Raises
        ------
        ConnectionError
            if the connection is not valid or the database does not exist or the namespace does not exist or the username or password is wrong or the database is not accessible raise ConnectionError

        Returns
        -------
        None
        """
        await self.query("INFO FOR DB;")

    async def __post(
        self, *extends: tuple[str], query: str
    ) -> Union[dict[str, Any], list[Any]]:
        """
        Execute a query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)
        extends : tuple[str]
            the extensions to add to the base url of the database connection.

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the query as a list of dictionaries or a dictionary if the query is valid
        """
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        response: httpx.Response = await self.__session.post(extend_uri, content=query)
        if response.status_code == 200:
            return json_loads(await response.aread())
        elif response.status_code == 400:
            logger.error(await response.aread())
            raise SurrealError(json_loads(await response.aread())["information"])
        else:
            logger.error(await response.aread())
            return None

    async def __get(self, *extends: tuple[Any]) -> Union[dict[str, Any], list[Any]]:
        """
        Make a get request to the database. If the request is not valid, raise an exception. If the request is valid, return the result.

        Parameters
        ----------
        extends : tuple[Any]
            the extensions to add to the base url of the database connection.

        Raises
        ------
        SurrealError
            if the request is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the request as a list of dictionaries or a dictionary if the request is valid or None if the request is not valid.
        """
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        response: httpx.Response = await self.__session.get(extend_uri)
        logger.debug(await response.aread())
        if response.status_code == 200:
            return json_loads(await response.aread())
        else:
            return None

    def __transform_into_sql_val(val: Any) -> Optional[Any]:
        """
        Transform a value into a SQL value.

        Parameters
        ----------
        val : Optional[Any]
            the value to transform into a SQL value as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones

        Returns
        -------
        Any
            the value as a SQL value as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones.
        """
        if isinstance(val, str):
            return "'%s'" % val
        return val

    async def raw_query(self, query: str) -> Union[dict[str, Any], list[Any]]:
        """
        Execute raw query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the query as a list of dictionaries or a dictionary if the query is valid or None if the query is not valid.
        """
        # execute a raw query
        return await self.__post("sql", query=query)

    async def mapped_query(self, query: str) -> SurrealResponse:
        """
        Execute mapped query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object
        """
        # execute a mapped query
        return SurrealResponse(await self.raw_query(query))

    async def query(self, query: str, **kwargs) -> SurrealResponse:
        """
        Execute a query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)
        kwargs : dict[str,Any]
            the values to replace in the query as a dictionary

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object
        """
        # query the database
        return await self.mapped_query(query.format(**kwargs))

    async def let(self, key, val) -> None:
        """
        Let a value in the database. If the value is not valid, raise an exception. If the value is valid, return the result.

        Parameters
        ----------
        key : str
            the key to let as a string
        val : Any
            the value to let as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones

        Raises
        ------
        SurrealError
            if the value is not valid, raise SurrealError with the error message
        """
        return await self.mapped_query(
            "LET ${}={}".format(key, self.__transform_into_sql_val(val))
        )

    async def select(self, tid: str) -> SurrealResponse:
        """
        Select a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.
        tid is the id of a table or table name. If the tid is not valid, raise an exception

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return await self.mapped_query("SELECT * FROM {}".format(tid))

    async def create(self, tid: str, data: Optional[dict] = None) -> SurrealResponse:
        """
        Create a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            await self.mapped_query(f"CREATE {tid} CONTENT {json_dumps(data)}")
            if data is not None
            else await self.mapped_query(f"CREATE {tid}")
        )

    async def update(self, tid: str, data: Optional[dict] = None):
        """
        Update a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else await self.mapped_query(f"UPDATE {tid} CONTENT {json_dumps(data)}")
        )

    async def change(self, tid: str, data: Optional[dict] = None):
        """
        Change a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else await self.mapped_query(f"UPDATE {tid} MERGE {json_dumps(data)}")
        )

    async def modify(self, tid: str, data: Optional[dict] = None):
        """
        Modify a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else await self.mapped_query(f"UPDATE {tid} PATCH {json_dumps(data)}")
        )

    async def delete(self, tid: str):
        """
        Delete a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return await self.mapped_query(f"DELETE * FROM {tid}")


class Client:
    """
    This class is a wrapper for the SurrealDB HTTP API.
    ...
    Parameters
    ----------
    uri : str
        The URI of the SurrealDB server.
    namespace : str
        The namespace of the database.
    database : str
        The name of the database.
    username : str
        The username of the database.
    password : str
        The password of the database.

    Attributes
    ----------
    uri : str
        the uri of the database
    namespace : str
        the namespace of the database
    database : str
        the database name
    username : str
        the username of the database
    password : str
        the password of the database

    """

    def __init__(
        self, uri: str, namespace: str, database: str, *, username: str, password: str
    ) -> None:
        """
        Constructor of the class.

        Parameters
        ----------
        uri : str
            The URI of the SurrealDB server.
        namespace : str
            The namespace of the database.
        database : str
            The name of the database.
        username : str
            The username of the database.
        password : str
            The password of the database.
        """
        self.__session = httpx.Client(
            auth=httpx.BasicAuth(username=username, password=password)
        )
        self.__session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "NS": namespace,
                "DB": database,
            }
        )
        self.__uri = uri
        self.__baseurl = base_url(uri)
        self.__check()
        self.__ns = namespace
        self.__db = database

    @property
    def ns(self) -> str:
        """
        The namespace of the database.

        Returns
        -------
        str
            the namespace of the database
        """
        return self.__ns

    @property
    def db(self) -> Union[dict[str, Any], list[Any]]:
        """
        The databases in the namespace.

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the databases in the namespace as a list of dictionaries or a dictionary
        """
        return self.__db

    @property
    def tables(self) -> Union[dict[str, Any], list[Any]]:
        """
        The tables in the database.

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the tables in the database as a list of dictionaries or a dictionary
        """
        # retrieve all tables
        data = self.raw_query("INFO FOR DB;")
        for i in data[0]["result"]["tb"].keys():
            yield i

    @property
    def uri(self) -> str:
        """
        The URI of the SurrealDB server.

        Returns
        -------
        str
            the URI of the SurrealDB server as a string
        """
        return self.__uri

    def __check(self) -> None:
        """
        Check if the connection is valid. If not, raise an exception.

        Raises
        ------
        ConnectionError
            if the connection is not valid or the database does not exist or the namespace does not exist or the username or password is wrong or the database is not accessible raise ConnectionError

        Returns
        -------
        None
        """
        self.query("INFO FOR DB;")

    def __post(
        self, *extends: tuple[str], query: str
    ) -> Union[dict[str, Any], list[Any]]:
        """
        Execute a query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)
        extends : tuple[str]
            the extensions to add to the base url of the database connection.

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the query as a list of dictionaries or a dictionary if the query is valid
        """
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        response: httpx.Response = self.__session.post(extend_uri, content=query)
        logger.debug(response.text)
        if response.status_code == 200:
            return json_loads(response.text)
        elif response.status_code == 400:
            logger.error(response.text)
            raise SurrealError(json_loads(response.text)["information"])
        else:
            logger.error(response.text)
            return None

    def __get(self, *extends: tuple[Any]) -> Union[dict[str, Any], list[Any]]:
        """
        Make a get request to the database. If the request is not valid, raise an exception. If the request is valid, return the result.

        Parameters
        ----------
        extends : tuple[Any]
            the extensions to add to the base url of the database connection.

        Raises
        ------
        SurrealError
            if the request is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the request as a list of dictionaries or a dictionary if the request is valid or None if the request is not valid.
        """
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        response: httpx.Response = self.__session.get(extend_uri)
        logger.debug(response.text)
        if response.status_code == 200:
            return json_loads(response.text)
        else:
            return None

    def __transform_into_sql_val(val: Any) -> Optional[Any]:
        """
        Transform a value into a SQL value.

        Parameters
        ----------
        val : Optional[Any]
            the value to transform into a SQL value as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones

        Returns
        -------
        Any
            the value as a SQL value as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones.
        """
        if isinstance(val, str):
            return "'%s'" % val
        return val

    def raw_query(self, query: str) -> Union[dict[str, Any], list[Any]]:
        """
        Execute raw query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        Union[dict[str,Any],list[Any]]
            the result of the query as a list of dictionaries or a dictionary if the query is valid or None if the query is not valid.
        """
        # execute a raw query
        return self.__post("sql", query=query)

    def mapped_query(self, query: str) -> SurrealResponse:
        """
        Execute mapped query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object
        """
        # execute a mapped query
        return SurrealResponse(self.raw_query(query))

    def query(self, query: str, **kwargs) -> SurrealResponse:
        """
        Execute a query. If the query is not valid, raise an exception. If the query is valid, return the result.

        Parameters
        ----------
        query : str
            the query to execute as a string in surrealql format (https://surrealdb.com/docs/surrealql)
        kwargs : dict[str,Any]
            the values to replace in the query as a dictionary

        Raises
        ------
        SurrealError
            if the query is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object
        """
        # query the database
        return self.mapped_query(query.format(**kwargs))

    def let(self, key, val) -> None:
        """
        Let a value in the database. If the value is not valid, raise an exception. If the value is valid, return the result.

        Parameters
        ----------
        key : str
            the key to let as a string
        val : Any
            the value to let as a string, int, float, bool, None or a list of strings, ints, floats, bools or Nones

        Raises
        ------
        SurrealError
            if the value is not valid, raise SurrealError with the error message
        """
        return self.mapped_query(
            "LET ${}={}".format(key, self.__transform_into_sql_val(val))
        )

    def select(self, tid: str) -> SurrealResponse:
        """
        Select a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.
        tid is the id of a table or table name. If the tid is not valid, raise an exception

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return self.mapped_query("SELECT * FROM {}".format(tid))

    def create(self, tid: str, data: Optional[dict] = None) -> SurrealResponse:
        """
        Create a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            self.mapped_query(f"CREATE {tid} CONTENT {json_dumps(data)}")
            if data is not None
            else self.mapped_query(f"CREATE {tid}")
        )

    def update(self, tid: str, data: Optional[dict] = None):
        """
        Update a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} CONTENT {json_dumps(data)}")
        )

    def change(self, tid: str, data: Optional[dict] = None):
        """
        Change a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} MERGE {json_dumps(data)}")
        )

    def modify(self, tid: str, data: Optional[dict] = None):
        """
        Modify a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} PATCH {json_dumps(data)}")
        )

    def delete(self, tid: str):
        """
        Delete a tid in the database. If the tid is not valid, raise an exception. If the tid is valid, return the result.

        Parameters
        ----------
        tid : str
            the tid to select as a string. tid is the id of a table or table name
        data : Optional[dict] (default: None)
            the data to create as a dictionary

        Raises
        ------
        SurrealError
            if the tid is not valid, raise SurrealError with the error message

        Returns
        -------
        SurrealResponse
            the result of the query as a SurrealResponse object

        """
        return self.mapped_query(f"DELETE * FROM {tid}")

    async def __aenter__(self) -> "Client":
        """
        Enter the async context manager. Return the client.

        Returns
        -------
        Client
            the client instance
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[BaseException] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        """
        Exit the async context manager. Close the connection.

        Parameters
        ----------
        exc_type : Optional[BaseException] (default: None)
            the exception type
        exc_value : Optional[BaseException] (default: None)
            the exception value
        traceback : Optional[TracebackType] (default: None)
            the traceback type
        """
        await self.disconnect()

    async def connect(self) -> None:
        """
        Connect to the database. If the connection is already established, raise an exception.
        """
        await self.__session.__aenter__()

    async def disconnect(self) -> None:
        """
        Disconnect from the database. If the connection is already closed, raise an exception. If the connection is not closed, close the connection.
        """
        await self.__session.aclose()

__all__ = ["Client","AsyncClient"]