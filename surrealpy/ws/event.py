import enum
from types import NoneType
from typing import Any, Callable, Optional, Union
import uuid
from .models import SurrealResponse

__all__ = ("Event", "Events", "managers")

managers: dict[str, "_EventManager"] = {}


class Events(enum.Enum):
    """
    A class used to represent SurrealDB events.
    ...
    Attributes
    ----------
    CONNECTED: str
        The event that is called when the client is connected to the server
    DISCONNECTED: str
        The event that is called when the client is disconnected from the server
    LOGIN: str
        The event that is called while the client is logging to the server
    LOGOUT: str
        The event that is called when the client is logged out from the server
    LOGGED_IN: str
        The event that is called when the client is logged in to the server
    USE: str
        The event that is called when the client is switched to a different database or collection
    RECEIVED: str
        The event that is called when the client receives a response from the server
    ERROR: str
        The event that is called when the client receives an error from the server
    """

    CONNECTED = "connected" # Implemented
    DISCONNECTED = "disconnected" # Implemented
    LOGIN = "login" # Implemented
    LOGGED_IN = "logged_in" # Implemented
    LOGOUT = "logout" # Not implemented
    USE = "use" # Implemented
    RECEIVED = "received" # Implemented
    ERROR = "error" # Not Completely Implemented
class Event:
    """
    A class used to represent SurrealDB event.
    ...
    Attributes
    ----------
    event: Events
        The event that is called
    response: SurrealResponse
        The response that is returned
    """

    def __init__(
        self,
        event: Events,
        response: Optional[Union[SurrealResponse, Any]] = None,
        *,
        id: str = None,
    ):
        self.id: str = id or response.id
        self.event: Events = event
        self.response: Optional[Union[SurrealResponse, Any]] = response

    def __repr__(self):
        return f"Event({self.event}, {self.response})"


class _EventManager:
    """
    A class used to represent SurrealDB events manager.
    ...
    Attributes
    ----------
    events: dict[str,set]
        A dict of events and their callbacks


    """

    def __init__(self):
        self._name = uuid.uuid4().hex
        managers[self._name] = self
        self.events: dict[str, set] = {}

    def add_event(self, event: str, callback: Callable[[SurrealResponse], Any]):
        """
        Add an event to the events manager.
        Parameters
        ----------
        event: str
            The event name
        callback: Callable[[SurrealResponse], Any]
            The callback function
        """
        if event not in self.events:
            self.events[event] = set()

        self.events[event].add(callback)

    def remove_event(self, event: str, callback: Callable[[SurrealResponse], Any]):
        """
        Remove an event from the events manager.
        Parameters
        ----------
        event: str
            The event name
        callback: Callable[[SurrealResponse], Any]
            The callback function
        """
        if event in self.events:
            self.events[event].remove(callback)
        else:
            raise ValueError(f"Event {event} is not registered")
        if "all" in self.events and callback in self.events["all"]:
            self.events["all"].remove(callback)

    def clear(self):
        """
        Clear all events from the events manager.
        """
        self.events.clear()

    def emit(self, event: str, data: "Event"):
        """
        Emit an event to the events manager.
        Parameters
        ----------
        event: str
            The event name
        data: SurrealResponse
            The data to pass to the callback function
        """

        if isinstance(event, Events):
            event = event.value

        if event in self.events:
            for callback in self.events[event]:
                callback(data)
            for callback in self.events.get("all", []):
                callback(data)

    def on(self, event: Union[str, Callable[[Event], None]]) -> Callable:
        """
        A decorator to add an event to the events manager.
        Parameters
        ----------
        event: Union[str,Callable]
            The event name or the callback function
        Returns
        -------
        Callable
            The decorator
        """
        if callable(event):
            self.add_event(event.__name__, event)
            return event
        else:

            def decorator(func: Callable):
                self.add_event(event, func)
                return func

            return decorator

    def __del__(self):

        del managers[self._name]
