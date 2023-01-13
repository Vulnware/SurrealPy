"""
This is a CLI for interacting with the Surreal API.
"""
import cmd
import dataclasses
import typing
import traceback
from surrealpy import ws, exceptions as ex
import argparse as ap
import pprint
import json
from pyfiglet import Figlet
import sys
from surrealpy.utils import json_dumps, pprint, cpprint, cprint, colored, COLORS
from surrealpy.ws.models import LoginParams, SurrealResponse

DEBUG = True


f = Figlet(font="slant")
cprint(f.renderText("SurrealDB CLI"), "magenta")


def passify_exception(func: typing.Callable[..., typing.Any]) -> typing.Callable:
    """
    This decorator will passify the exception. This will print the exception message instead of raising it. This is useful for the CLI.

    Parameters
    ----------
    func : typing.Callable

    """

    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            # use traceback instead of logging because we don't want to raise exceptions in the CLI
            "exception class: exception message"
            if DEBUG:
                traceback.print_exc()
            else:
                cprint("{}: {}".format(e.__class__.__name__, e), "red", attrs=["blink"])

    # keep the name and docstring of the function to make it easier to documenting
    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__ or "Not Documented Yet"
    return wrapper


class SurrealCLI(cmd.Cmd):
    # make command start with '@' symbol
    def __init__(self, conn: typing.Optional[ws.SurrealClient] = None):
        super().__init__()
        self.active_connections: dict[str, ws.SurrealClient] = {}
        self.active_connection: ws.SurrealClient = conn
        self.active_name: str = None
        self.prompt = colored("SurrealDB> ", "magenta")
        if conn is not None:
            self.set_active_connection("DEFAULT", conn)
            self.set_active_database(conn.namespace, conn.database, "DEFAULT")
            self.active_connections["DEFAULT"] = conn

    def do_pass(self, inp: typing.Optional[typing.Any] = None) -> None:
        return None

    @passify_exception
    def use(self, inp: str) -> None:
        """
        A shortcut to use
        """
        inp = inp[1:]
        inps = inp.split(".")
        if len(inps) == 1:
            # use database
            self.active_connection.use(
                namespace=self.active_connection.namespace, database=inps[0]
            )
            self.set_active_database(
                self.active_connection.namespace, inps[0], self.active_name
            )

        elif len(inps) == 2:
            # use namespace
            self.active_connection.use(namespace=inps[0], database=inps[1])
            self.set_active_database(inps[0], inps[1], self.active_name)
        elif len(inps) == 3:
            # use connection
            try:
                self.active_connection = self.active_connections[inps[0]]
            except KeyError:
                raise ex.SurrealDBCliError(f"Connection {inps[0]} does not exist")
            self.active_connection.use(namespace=inps[1], database=inps[2])
            self.set_active_database(inps[1], inps[2], inps[0])
        else:
            raise ex.SurrealDBCliError("Invalid use statement")

    def do_indent(self, inp: str):
        self.indent = int(inp)

    def set_active_connection(self, name: str, conn: ws.SurrealClient):
        self.active_connection = conn
        self.prompt = colored(f"{name}> ", "magenta")
        self.active_name = name

    def set_active_database(self, namespace: str, database: str, conn_name: str):
        self.prompt = colored(f"{conn_name}.{namespace}.{database}> ", "magenta")

    def parseline(self, line):
        """Parse the line into a command name and a string containing
        the arguments.  Returns a tuple containing (command, args, line).
        'command' and 'args' may be None if the line couldn't be parsed.
        """
        line = line.strip()
        if not line:
            return None, None, line
        elif line[0] == "@":
            i, n = 1, len(line)
            while i < n and line[i] != " ":
                i = i + 1
            arg = line.strip()

            return "exc", arg[1:], line
        elif line[0] == ".":
            i, n = 1, len(line)
            while i < n and line[i] != " ":
                i = i + 1
            arg = line.strip()

            self.use(arg)

            return "pass", arg[1:], line
        elif line[0] == "?":
            line = "help " + line[1:]
        elif line[0] == "!":
            if hasattr(self, "do_shell"):
                line = "shell " + line[1:]
            else:
                return None, None, line
        i, n = 0, len(line)
        while i < n and line[i] in self.identchars:
            i = i + 1
        cmd, arg = line[:i], line[i:].strip()
        return cmd, arg, line

    def do_exit(self, inp: typing.Optional[typing.Any]):
        """
        Will exit the application and also disconnect the all active connections

        Usage: exit

        Example: exit

        PARAMETERS
        ----------
        inp : typing.Optional[typing.Any]
            The input that is passed to the command. This is not used.
        """
        for name, conn in self.active_connections.items():
            conn.disconnect()
            print(f"Disconnected from {name}")
        print("All active connections closed")
        return True

    @passify_exception
    def do_execute(self, inp: str) -> None:
        """
        Execute a query and return a list of results from the query result

        Usage: execute <query>

        Example: execute SELECT * FROM human

        PARAMETERS
        ----------
        query: str
            The query to execute.
        """
        if inp == "":
            raise ex.SurrealDBCliError(
                "No SurrealQL given. Please enter a SurrealQL query."
            )
        # will use pprint() instead of print because pprint() is more readable
        if self.active_connection is None:
            raise ex.SurrealDBCliError(
                "No active connection. Please connect to a database first."
            )
        responses: SurrealResponse = self.active_connection.query(inp)
        for response in responses.results:
            if response:
                for i, result in enumerate(response):
                    if i + 1 >= 50:
                        cprint("...", "red")
                        break
                    cprint("-" * 80, color="yellow")

                    if isinstance(result, dict):
                        for k, v in result.items():
                            cprint(f"{k}: {v}", color="cyan")
                    else:
                        cprint(result, color="cyan")
                cprint("-" * 80, color="yellow")
            else:
                cprint("No results", color="red")

    def do_exc(self, inp: str) -> None:
        """
        A shortcut for execute
        """
        self.do_execute(inp)

    @passify_exception
    def do_connections(self, inp: typing.Optional[typing.Any] = None) -> None:
        """
        Get a list of connections from the server

        Usage: connections

        Example: connections

        PARAMETERS
        ----------
        None
        """
        if len(self.active_connections.keys()) == 0:
            print("No connections are connected")
        else:

            print(f"STATUS{' '*12}ACTIVE\tNAME\tURL")
            for name, client in self.active_connections.items():
                text = "[ ]"
                color = "red"
                if self.active_name == name:
                    text = "[x]"
                    color = "green"
                if client.ws.connected:
                    s_text = "CONNECTED"
                    s_color = "green"
                else:
                    s_text = "DISCONNECTED"
                    s_color = "red"
                print(
                    colored(s_text, s_color),
                    colored(text, color),
                    colored(name, "cyan"),
                    colored(client.url, "yellow"),
                    sep="\t",
                )
                # print(f"{name}\t{client.url}")

    def do_cns(self, inp: typing.Optional[typing.Any] = None) -> None:
        """
        A shortcut for connections
        """
        self.do_connections(inp)

    @passify_exception
    def do_info(self, inp: typing.Optional[typing.Any] = None) -> None:
        """
        Get information about the active database of active connection

        Usage: info

        Example: info

        PARAMETERS
        ----------
        None
        """

        pprint.pprint(self.active_connection.info())

    def do_inf(self, inp: typing.Optional[str] = None) -> None:
        """
        A shortcut for info
        """
        self.do_info(inp)

    @passify_exception
    def do_connect(self, inp: str):
        """
        Connect to a surrealDB Server

        Usage: connect <name> <url> (optional)

        Example: connect DEFAULT ws://localhost:8000/rpc or connect DEFAULT (if only name is given then active connection will be changed to given connection)

        PARAMETERS
        ----------
        name: str
            The name of the surrealDB Server
        url: str
            The url of the surrealDB Server
        """
        parameters = inp.split(" ")
        if len(parameters) == 1:
            if parameters[0] in self.active_connections:
                self.set_active_connection(
                    parameters[0], self.active_connections[parameters[0]]
                )
                return None
            else:
                raise ex.SurrealDBCliError(f"Connection {parameters[0]} does not exist")
        elif len(parameters) > 2:
            raise ex.SurrealDBCliError(
                "Too many parameters.\nUsage: connect <name> <url>"
            )
        name, url = parameters
        self.active_connections[name] = ws.SurrealClient(url)
        self.active_connections[name].connect()
        self.set_active_connection(name, self.active_connections[name])
        print(f"Connected to {name} at {url}")

    def do_conn(self, inp: str):
        """
        A shortcut to connect
        """
        self.do_connect(inp)

    @passify_exception
    def do_login(self, inp: str):
        """
        Login to a database

        Usage: login <username> <password> <name> (optional)

        Example: login admin admin DEFAULT or admin admin (will use active connection)

        PARAMETERS
        ----------
        username: str
            The username to login
        password: str
            The password to login
        name: str (optional)
            The name of the connection to login to. If not specified, the active connection will be used.
        """
        name, username, password = None, None, None
        parameters = inp.split(" ")
        if len(parameters) < 2:
            raise ex.SurrealDBCliError(
                "Not enough parameters for login\nUsage: login <name> <username> <password>"
            )
        if len(parameters) == 2:
            username, password = parameters
            name = self.active_name
        else:
            username, password, name = parameters

        self.active_connections[name].login(LoginParams(username, password))
        print(f"Logged in to {name} as {username}")

    @passify_exception
    def do_use(self, inp: str):
        """
        Use a database on active connection

        Usage: use <namespace> <database>

        Example: use testNameSpace testDatabase

        PARAMETERS
        ----------
        namespace: str
            The name of the database to use
        database: str
            The name of the database to use
        """
        if not self.active_connection:
            raise ex.SurrealDBCliError("No active connection")
        parameters = inp.split(" ")
        if len(parameters) != 2:
            raise ex.SurrealDBCliError(
                "Not enough parameters for use\nUsage: use <namespace> <database>"
            )
        namespace, database = parameters
        self.active_connection.use(namespace, database)
        print(f"Using {namespace}.{database} on {self.active_name}")
        self.set_active_database(namespace, database, self.active_name)


if __name__ == "__main__":
    """
    This is the main function of the CLI. It will start the CLI.
    """

    """
    REQUIRED PARAMETERS
    -------------------
    url: str
        The URL of the Surreal API. This is required if you want to connect right away
    namespace: str
        The namespace of the database. This is required if you want to connect right away.
    database: str
        The name of the database. This is required if you want to connect right away.
    username: str
        The username of the database. This is required if you want to connect right away.
    password: str
        The password of the database. This is required if you want to connect right away.
    """
    parser = ap.ArgumentParser(
        description="Command line Tool for SurrealPy API", prog="surrealpy cli"
    )
    parser.add_argument(
        "-u", "--url", dest="url", default=None, help="The URL of the Surreal API"
    )
    parser.add_argument(
        "-n",
        "--namespace",
        dest="namespace",
        default=None,
        help="The namespace of the database",
    )
    parser.add_argument(
        "-d",
        "--database",
        dest="database",
        default=None,
        help="The name of the database",
    )
    parser.add_argument(
        "-U",
        "--username",
        dest="username",
        default=None,
        help="The username of the database",
    )
    parser.add_argument(
        "-p",
        "--password",
        dest="password",
        default=None,
        help="The password of the database",
    )
    args = parser.parse_args()
    if any(
        [args.url, args.namespace, args.database, args.username, args.password]
    ) and not all(
        [args.url, args.namespace, args.database, args.username, args.password]
    ):
        print("You must provide all the required parameters")
        exit(1)
    if all([args.url, args.namespace, args.database, args.username, args.password]):
        db = ws.SurrealClient(args.url)
        db.connect()
        db.login(ws.models.LoginParams(args.username, args.password))
        db.use(args.namespace, args.database)
        cli = SurrealCLI(db)
    else:
        cli = SurrealCLI()
    cli.cmdloop()
