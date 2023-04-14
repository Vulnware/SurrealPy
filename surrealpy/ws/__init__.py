import dataclasses
import threading
import time
from typing import Any, Callable, Optional, Tuple, Union
import warnings
from websocket import create_connection, WebSocket
from surrealpy.ws.models import LoginParams, SurrealRequest, SurrealResponse
from surrealpy.utils import  escape_sql_params, json_dumps, json_loads
from surrealpy.exceptions import SurrealError, SurrealQLSyntaxError, WebSocketError
from surrealpy.ws import event
import atexit
import logging
import queue  # noqa: F401 # pylint: disable=unused-import

logger = logging.getLogger("surrealpy.ws")
# logger will work only if debug is enabled
logger.setLevel(logging.DEBUG)
# add null handler to logger to prevent errors 
logger.addHandler(logging.NullHandler())


__all__ = ("SurrealClient", "SurrealClientThread")

def unthread(func):
    """
    Decorator to run a function in the main thread.

    Parameters
    ----------
    func : function
        The function to run in the main thread.

    Returns
    -------
    function
        The function to run in the main thread.
    """

    def wrapper(*args: Any, **kwargs: dict[str, Any]) -> Any:
        """
        Wrapper function.

        Parameters
        ----------
        *args : Any
            The arguments to pass to the function.
        **kwargs : dict[str,Any]
            The keyword arguments to pass to the function.

        Returns
        -------
        Any
            The return value of the function.

        Raises
        ------
        RuntimeError
            If the function is not run in the main thread.
        """
        assert (
            threading.current_thread() is threading.main_thread()
        ), "This function is not thread safe"

        return func(*args, **kwargs)

    # keep the name of the function and docstring for documentation
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__ or "Not Documented Yet"
    warnings.warn("This function is deprecated", DeprecationWarning)
    return wrapper


class SurrealClient:
    """
    A class used to represent SurrealClient
    ...

    note: This class is not thread safe

    Attributes
    ----------
    url: str
        The url of the websocket server
    ws: WebSocket
        The websocket connection
    namespace: Optional[str]
        The namespace of the database
    database: Optional[str]
        The database name

    Methods
    -------
    connect() -> None
        Connect to the SurrealDB server
    disconnect() -> None
        Disconnect from the SurrealDB server
    ping() -> str
        Ping the SurrealDB server
    use(namespace: str, database: str) -> None
        Use a database
    info() -> dict[str, Any]
        Get current SurrealDB server's authentication info
    register(params: dict[str, Any]) -> str
        Signup to the SurrealDB server
    login(params: dict[str, Any]) -> None
        Login to the SurrealDB server
    invalidate() -> None
        Invalidate the current session
    authenticate(token: str) -> None
        Authenticate the current session
    killProcess(id: str) -> None
        Kill the given id process
    let(key: str, value: Any) -> None
        Set a let variable to the SurrealDB server
    query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]
        Query the SurrealDB server with the given query and params (optional) that returns a list of dict
    find(tid: str) -> list[dict[str, Any]]
        Find the given tid (table or record id) in the SurrealDB server and returns a list of dict
    find_one(tid: str) -> dict[str, Any]
        Find the given tid (table or record id) in the SurrealDB server and returns a dict
    create(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Create a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the created record(s)
    update(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Update a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the updated record(s)
    change(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Change a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the changed record(s)
    modify(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Modify a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the modified record(s)
    delete(tid: str) -> list[dict[str, Any]]
        Delete a record in the SurrealDB server with the given tid (table id), returns a list of dict of the deleted record(s)
    """

    def __init__(self, url):
        """
        Initialize the SurrealClient instance.

        Parameters
        ----------
        url : str
            The url of the websocket server
        """
        warnings.warn("This class is deprecated instead use SurrealClientThread which will be referred  as SurrealClient also", DeprecationWarning)
        print("This class is deprecated instead use SurrealClientThread which will be referred as SurrealClient also")
        if url.startswith("http://"):
            url = url.replace("http://", "ws://")
        elif url.startswith("https://"):
            url = url.replace("https://", "wss://")
        self.__url: str = url
        self._ws: WebSocket
        self._counter: int = 0
        self._namespace: Optional[str] = None
        self._database: Optional[str] = None
        self._let_variables: list[str] = set()
        # atexit.register(self._atexit)

    def _atexit(self):
        """
        This is a private function that is used to disconnect the websocket connection when the program exits. It is not recommended to use this function.
        """

        self.disconnect()

    def _count(self) -> str:
        """
        This is a private function that is used to count the number of requests. It is not recommended to use this function.

        Returns
        -------
        str
            The number of requests
        """
        self._counter += 1
        return str(self._counter)

    def _set_let(self, key: str):
        """
        This is a private function that is used to set the let variables. It is not recommended to use this function. May use in the future.

        Parameters
        ----------
        key: str
            The key of the let variable
        """
        self._let_variables.add(key)

    @property
    def ws(self) -> WebSocket:
        """The websocket connection.

        Returns
        -------
        WebSocket
            The websocket connection"""
        return self._ws

    @property
    def namespace(self):
        """The namespace of the database.

        Returns
        -------
        Optional[str]
            The namespace of the database
        """
        return self._namespace

    @property
    def database(self):
        """The database name.

        Returns
        -------
        Optional[str]
            The database name
        """
        return self._database

    @property
    def url(self):
        """The url of the websocket server.

        Returns
        -------
        str
            The url of the websocket server
        """
        return self.__url

    def info(self) -> Any:
        """
        The info about connected database connection

        Returns
        -------
        Any
            The info about connected database connection
        """
        return self.query("INFO DB;")


    def connect(self) -> None:
        """
        Connect to the SurrealDB server.

        Raises
        ------
        WebSocketError
            If the connection is already established
        """



        if hasattr(self, "ws") and self.ws.connected:
            raise WebSocketError("Already connected")

        self._ws = create_connection(self.url)


    def disconnect(self) -> None:
        """
        Disconnects from the websocket server.

        Raises
        ------
        WebSocketError
            If the connection is not established

        """
        if hasattr(self, "ws") and self.ws.connected:
            self.ws.close()

        else:
            raise WebSocketError({"message": "Not connected", "code": -1})

    def _raw_send(self, request: SurrealRequest) -> dict[str, Any]:
        """
        This is a private function that is used to send the request to the SurrealDB server. It is not recommended to use this function. May be deprecated in the future.

        Parameters
        ----------
        request: SurrealRequest
            The request to send to the SurrealDB server

        Raises
        ------
        WebSocketException
            If the connection is not established

        Returns
        -------
        dict[str, Any]
            The response from the SurrealDB server
        """
        self.ws.send(json_dumps(request))
        return json_loads(self.ws.recv())


    def _send(self, method: str, *params: Any) -> Tuple[Union[int,str],Union[list,dict]]:
        """
        Sends a request to the websocket server

        Parameters
        ----------
        method: str
            The method of the request
        *params: Any
            The parameters of the request

        Raises
        ------
        WebSocketError
            if surrealDB server returns an error(s)

        Returns
        -------
        Any
            The response from the SurrealDB server
        """
        request = SurrealRequest(id=self._count(), method=method, params=params)
        self.ws.send(json_dumps(dataclasses.asdict(request)))
        data = self.ws.recv()
        returnData = json_loads(data)
        if returnData.get("error") is not None:
            if returnData["error"]["code"] == -32000:
                err = SurrealQLSyntaxError(returnData["error"]["message"], params[0])
            else:
                err = WebSocketError(returnData["error"])
            raise err
        return returnData["id"], returnData["result"]

    def _clean_dict_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Clean the params of the request. It is not recommended to use this function. May cause unexpected behavior.

        Parameters
        ----------
        params: dict[str, Any]
            The params to clean

        Returns
        -------
        dict[str, Any]
            The cleaned params
        """
        return {k: v for k, v in params.items() if v is not None}

    def ping(self) -> bool:
        """
        Ping the SurrealDB server.

        Returns
        -------
        bool
            True if the SurrealDB server is alive else False
        """
        return self._send("ping")[1]

    def use(self, namespace: str, database: str) -> None:
        """
        Use a database.

        Parameters
        ----------
        namespace: str
            The namespace of the database
        database: str
            The database name

        """
        returnData = self._send("use", namespace, database)[1]
        self._namespace = namespace
        self._database = database
        return returnData

    def register(self, params: dict[str, Any]) -> str:
        """
        Signup to the SurrealDB server.

        Parameters
        ----------
        params: dict[str, Any]
            The params of the request

        Returns
        -------
        str
            The token of the user
        """
        clean_params = self._clean_dict_params(params)
        return self._send("signup", clean_params)[1]

    def login(self, params: Union[dict[str, Any], LoginParams]) -> None:
        """
        Login to the SurrealDB server.

        Parameters
        ----------
        params: Union[dict[str, Any], LoginParams]
            The credentials of the login request
        """
        if isinstance(params, LoginParams):
            params = params.to_dict()
        clean_params = self._clean_dict_params(params)
        logger.debug(clean_params)
        return self._send("signin", clean_params)[1]

    def invalidate(self) -> None:
        """
        Invalidate the current session.
        """
        return self._send("invalidate")[1]

    def authenticate(self, token: str):
        """Authenticate the current session."""
        return self._send("authenticate", token)[1]

    def killProcess(self, id: str) -> None:
        """
        Kill the current process.
        """
        return self._send("kill", id)[1]

    def let(self, key: str, value: Any) -> None:
        """Set a let variable."""

        return self._send("let", key, value)[1]

    def query(
        self,
        sql: str,
        *,
        params: Union[dict[str, Any], list[Any], tuple[Any], set[Any]] = None,
    ) -> SurrealResponse:
        """Query the SurrealDB server.

        Parameters
        ----------
        sql: str
            The sql query to execute on the SurrealDB server
        params: Any
            The params of the query (optional)

        Returns
        -------
        SurrealResponse
            The result of the query as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)
        """
        if params is None:
            id, query_result = self._send("query", sql, params)
        elif isinstance(params, (list, tuple, set)):
            sql = sql.format(*escape_sql_params(params))
            print(sql)
            id, query_result = self._send("query", sql)
        elif isinstance(params, dict):
            sql = sql.format(**escape_sql_params(params))
            id, query_result = self._send("query", sql )
        else:
            raise TypeError("params must be a list, tuple, set or dict")
        return SurrealResponse(id=id, result=[r.get("result") for r in query_result])

    def find(self, tid: str) -> SurrealResponse:
        """Find documents by their ids or table name.
        Parameters
        ----------
        tid: str
            The id or table name of the document(s)

        Returns
        -------
        SurrealResponse
            The result of the query as a SurrealResponse object containing the id of the query and the result as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)
        """
        returnData = self._send("select", tid)
        return SurrealResponse(id=returnData[0], result=returnData[1])

    def find_one(self, tid: str) -> SurrealResponse:
        """Find a document by its id or table name.
        Parameters
        ----------
        tid: str
            The id or table name of the document

        Returns
        -------
        SurrealResponse
            The result of the query as a SurrealResponse object containing the result as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        # TODO: Will change documentation of return type
        response = self.find(tid)
        if len(response.result) == 0:
            return None
        
        return SurrealResponse(id=response.id,result=response.result[0])

    def create(
        self, tid: str, data: Union[Any, dict[str, Any]]
    ) -> SurrealResponse:
        """Create a document.
        Parameters
        ----------
        tid: str
            The id or table name of the document
        data: Union[Any,dict[str,Any]]
            The data of the document

        Returns
        -------
        list[dict[str,Any]]
            The document(s) created as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        # TODO: Will change documentation of return type

        _id,result = self._send("create", tid, data)
        return SurrealResponse(id=_id,result=result)

    def update(
        self, tid: str, data: Union[Any, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Update a document.
        Parameters
        ----------
        tid: str
            The id or table name of the document
        data: Union[Any,dict[str,Any]]
            The data of the document

        Returns
        -------
        list[dict[str,Any]]
            The document(s) updated as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        _id,result = self._send("update", tid, data)
        return SurrealResponse(id=_id,result=result)

    def change(
        self, tid: str, data: Union[Any, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Change a document.
        Parameters
        ----------
        tid: str
            The id or table name of the document
        data: Union[Any,dict[str,Any]]
            The data of the document

        Returns
        -------
        list[dict[str,Any]]
            The document(s) changed as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        _id,result = self._send("change", tid, data)
        return SurrealResponse(id=_id,result=result)

    def modify(
        self, tid: str, data: Union[Any, dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Modify a document.
        Parameters
        ----------
        tid: str
            The id or table name of the document
        data: Union[Any,dict[str,Any]]
            The data of the document

        Returns
        -------
        list[dict[str,Any]]
            The document(s) modified as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        _id,result = self._send("modify", tid, data)
        return SurrealResponse(id=_id,result=result)

    def delete(self, tid: str) -> list[dict[str, Any]]:
        """Delete document(s) by given tid.
        Parameters
        ----------
        tid: str
            The id or table name of the document

        Returns
        -------
        list[dict[str,Any]]
            The document(s) deleted as a list of dictionaries (rows) with the column names as keys and the values as values of the dictionary (row)

        """
        _id,result = self._send("delete", tid)
        return SurrealResponse(id=_id,result=result)


    def __enter__(self):
        """Enter the context manager.

        Returns
        -------
        SurrealClient
            The SurrealClient instance
        """
        self.connect()
        return self

    def __exit__(
        self, exc_type: Optional[Any], exc_val: Optional[Any], exc_tb: Optional[Any]
    ):
        """
        Exit the context manager.

        Parameters
        ----------
        exc_type: Any
            The exception type (optional) that was raised in the context manager (if any)
        exc_val: Any
            The exception value (optional) that was raised in the context manager (if any)
        exc_tb: Any
            The exception traceback (optional) that was raised in the context manager (if any)
        """
        self.disconnect()


class SurrealClientThread(SurrealClient):
    """
    A class used to represent SurrealClient
    ...

    note: This class is not thread safe

    Parameters
    ----------
    url: str
        The url of the SurrealDB server
    eventManager: Optional[event.EventManager]
        The event manager of the client (optional) if you want to use custom event manager (default is None)

    Attributes
    ----------
    url: str
        The url of the websocket server (e.g: http://127.0.0.1:8000/rpc)
    ws: WebSocket
        The websocket connection
    namespace: Optional[str]
        The namespace of the database
    database: Optional[str]
        The database name
    eventManager: Optional[event.EventManager]
        The event manager of the client (optional) if you want to use custom event manager (default is None)

    Methods
    -------
    connect() -> None:
        Connect to the SurrealDB server
    disconnect() -> None:
        Disconnect from the SurrealDB server
    ping() -> str:
        Ping the SurrealDB server
    use(namespace: str, database: str) -> None:
        Use a database
    info() -> dict[str, Any]:
        Get current SurrealDB server's authentication info
    register(params: dict[str, Any]) -> str
        Signup to the SurrealDB server
    login(params: dict[str, Any]) -> None
        Login to the SurrealDB server
    invalidate() -> None
        Invalidate the current session
    authenticate(token: str) -> None
        Authenticate the current session
    killProcess(id: str) -> None
        Kill the given id process
    let(key: str, value: Any) -> None
        Set a let variable to the SurrealDB server
    query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]
        Query the SurrealDB server with the given query and params (optional) that returns a list of dict
    find(tid: str) -> list[dict[str, Any]]
        Find the given tid (table or record id) in the SurrealDB server and returns a list of dict
    find_one(tid: str) -> dict[str, Any]
        Find the given tid (table or record id) in the SurrealDB server and returns a dict
    create(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Create a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the created record(s)
    update(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Update a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the updated record(s)
    change(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Change a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the changed record(s)
    modify(tid: str, data: Union[Any, dict[str, Any]]) -> list[dict[str, Any]]
        Modify a record in the SurrealDB server with the given tid (table id) and data, returns a list of dict of the modified record(s)
    delete(tid: str) -> list[dict[str, Any]]
        Delete a record in the SurrealDB server with the given tid (table id), returns a list of dict of the deleted record(s)
    on(event: Union[str, Callable]) -> Callable
        Decorator to register an event handler for the given event name or function name (if event is a function) to the EventManager and returns a function that can be used to unregister the event handler from the EventManager
    """

    def __init__(self, url: str, *, eventManager: Optional[event.EventManager] = None):
        """Initialize the SurrealClientThread instance.

        Parameters
        ----------
        url: str
            The url of the SurrealDB server

        """
        super().__init__(url)
        self._receive_thread: threading.Thread = threading.Thread(
            target=self._receive_responses, daemon=True
        )
        self._lock = threading.Lock()
        self._responses: dict[str, Any] = {}
        self._event_manager: event.EventManager = eventManager or event.EventManager()
        self._queue: dict[str, queue.Queue] = {} # Used to store the queues for each request id

        self.on = self._event_manager.on

    def count(self) -> int:
        """
        Count the number of responses.

        Returns
        -------
        int
            The number of responses
        """
        with self._lock:
            return len(self._responses)

    def _count(self) -> str:
        """
        This is a private function that is used to count the number of requests. It is not recommended to use this function.

        Returns
        -------
        str
            The number of requests
        """
        with self._lock:
            self._counter += 1
            return str(self._counter)

    def _receive_responses(self):
        """
        This is a private function that is used to receive responses from the SurrealDB server. It is not recommended to use this function.
        """
        while self.ws.connected:
            # Receive response from server and add it to the responses dictionary
            response = self._ws.recv()
            if response is None or response == "":
                # If the response is empty, then the connection has been closed
                continue
            # Parse the response as a dictionary
            response = json_loads(response)
            logger.debug(f"Received response: {response}")
            # Add the response to the responses dictionary
            if response.get("id", None) is not None:
                # self._responses[response["id"]] = response
                self._queue[response["id"]].put_nowait(response) # Put the response to the queue of the request id
                self._event_manager.emit(event.Events.RECEIVED, response)
            elif "error" in response:
                self._event_manager.emit(event.Events.ERROR, response)
                raise WebSocketError(response["error"])
            else:
                # NOTE: Not fully implemented yet
                # self._event_manager.emit(event.Events.LIVE, response) # Waiting for live query implementation in SurrealDB
                raise Exception(f"Unknown error: %s" % json_dumps(response))

    def _send(self, method: str, *params: Any) -> Tuple[Union[int,str],Union[list,dict]]:
        """
        Sends a request to the websocket server

        Parameters
        ----------
        method: str
            The method of the request
        *params: Any
            The parameters of the request

        Raises
        ------
        WebSocketError
            if surrealDB server returns an error(s)

        Returns
        -------
        Any
            The response from the SurrealDB server
        """
        request = SurrealRequest(id=self._count(), method=method, params=params)
        self._queue[request.id] = queue.Queue() # Create a queue for the request id
        self._ws.send(json_dumps(request))
        # Wait for the response to be received
        response = self._queue[request.id].get()
        if response is None:
            # If the response is None, then the connection has been closed
            err = SurrealError("Response is None")
            # Emit the error event
            self._event_manager.emit(
                event.Events.ERROR,
                event.Event(
                    event=event.Events.ERROR, response=SurrealResponse("-1", (err,))
                ),
            )
            raise err
        if response.get("error") is not None:
            # If the response has an error, then raise the error and return None
            if response["error"]["code"] == -32000:
                err = SurrealQLSyntaxError(response["error"]["message"], params[0])
            else:
                err = WebSocketError(response["error"])

            # Emit the error event
            self._event_manager.emit(
                event.Events.ERROR,
                event.Event(event=event.Events.ERROR, response=response, id=request.id),
            )
            raise err
        return response["id"], response["result"]

    def connect(self):
        """
        Connect to the SurrealDB server.

        Raises
        ------
        WebSocketError
            If the connection is already established
        """
        # check if thread is already running, if so, raise an error
        if self._receive_thread.is_alive():
            raise WebSocketError("Connection is already established")
        if not hasattr(self, "_ws"):
            # If the websocket is already initialized, then close it
            self._ws = create_connection(self.url)
            self._receive_thread.start()
            self._event_manager.emit(
                event.Events.CONNECTED,
                event.Event(
                    event.Events.CONNECTED,
                    response=SurrealResponse("-1", ("Connected",)),
                ),
            )

    def login(self, params: Union[dict[str, Any], LoginParams]) -> None:
        """
        Login to the SurrealDB server.

        Parameters
        ----------
        params: Union[dict[str, Any], LoginParams]
            The credentials of the login request
        """
        self._event_manager.emit(
            event.Events.LOGIN,
            event.Event(
                event.Events.LOGIN, response=SurrealResponse("-1", ("Logging in...",))
            ),
        )
        if isinstance(params, LoginParams):
            params = params.to_dict()
        clean_params = self._clean_dict_params(params)
        result = (self._send("signin", clean_params)[1],)
        self._event_manager.emit(
            event.Events.LOGGED_IN,
            event.Event(event.Events.LOGGED_IN, response=SurrealResponse("-1", result)),
        )
        return result

    def use(self, namespace: str, database: str) -> None:
        """
        Use a database.

        Parameters
        ----------
        namespace: str
            The namespace of the database
        database: str
            The database name

        """
        result = self._send("use", namespace, database)
        self._event_manager.emit(
            event.Events.USE,
            event.Event(
                event.Events.USE,
                response=SurrealResponse(
                    result[0], {"namespace": namespace, "database": database}
                ),
                id=result[0],
            ),
        )
        return result[1]

    def disconnect(self):
        """
        Disconnect from the SurrealDB server.

        Raises
        ------
        WebSocketError
            If the connection is already closed
        """
        if hasattr(self, "ws") and self.ws.connected:
            self._ws.close()
            self._receive_thread.join()
            self._event_manager.emit(
                event.Events.DISCONNECTED,
                event.Event(
                    event.Events.DISCONNECTED,
                    response=SurrealResponse("-1", ("Disconnected",)),
                ),
            )

        else:
            raise WebSocketError({"message": "Not connected", "code": -1})

    def __enter__(self):
        """Enter the context manager.

        Returns
        -------
        SurrealClient
            The SurrealClient instance
        """
        self.connect()
        return self

Client = SurrealClientThread # Alias for the SurrealClientThread class for backwards compatibility with the old version of SurrealDB
DeprecatedClient = SurrealClient # Alias for the SurrealClient class for backwards compatibility with the old version of SurrealDB