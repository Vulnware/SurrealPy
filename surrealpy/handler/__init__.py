import os
import threading
import subprocess as sp
import psutil
import shlex
import logging
import socket

# create logger object
logger = logging.getLogger("surrealpy.sync.client")

# if operation system is windows set process name to surreal.exe
if os.name == "nt":
    processName = "surreal.exe"
elif os.name == "posix":
    processName = "surreal"
else:
    print("Unknown OS")
    exit(1)


def checkIfProcessRunning(processName):
    """
    Check if there is any running process that contains the given name processName.
    """
    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if processName.lower() in proc.name().lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


# this is a thread safe singleton class
# with this class we can start a thread to handle the database process
def getAvaiblePort():
    sock = socket.socket()
    sock.bind(("", 0))

    return sock.getsockname()[1]


class SurrealDBHandler(threading.Thread):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        *,
        host: str = "0.0.0.0",
        port: int = None,
        scheme: str = "http",
    ) -> None:
        # check if surreal is already running
        threading.Thread.__init__(self, daemon=True)
        if checkIfProcessRunning(processName):
            print("Surreal is already running")
            exit(1)

        # check if uri is filepath
        self.__uri = uri
        self.__username = username
        self.__password = password
        self.__host = host
        self.__port = port or getAvaiblePort()
        self.__canrun = True
        self.__scheme = scheme
        if uri == "memory":
            self.__uri = "memory"
        else:
            self.__uri = "file://" + uri

        # start the database process
        self.__commandTemplate = "surreal start --log debug  --user {username} --pass {password} --bind {host}:{port} {uri}"
        self.__command = self.__commandTemplate.format(
            username=self.username,
            password=self.password,
            uri=self.__uri,
            host=host,
            port=port,
        )
        self.__parsedCommand = shlex.split(self.__command)
        # call the super class constructor

    def run(self):
        # start the database process
        self.__process = sp.Popen(self.__parsedCommand)
        # read the output of the process and log it
        while self.__canrun:
            output = self.__process.stdout.readline().decode()
            if output == "" and self.__process.poll() is not None:
                break
            if output:
                logger.debug(output.strip())
    def wait_until(self):
        while True:
            try:
                self.__process.wait(timeout=0.1)
                break
            except sp.TimeoutExpired:
                pass
    @property
    def uri(self):
        return self.__uri

    @property
    def command(self):
        return self.__command

    @property
    def username(self):
        return self.__username

    @property
    def password(self):
        return self.__password

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def bind(self):
        return self.__host + ":" + str(self.__port)

    @property
    def scheme(self):
        return self.__scheme

    def stop(self):
        self.__process.terminate()
        self.__process.wait()
        self.__canrun = False
        logger.debug("Surreal process terminated")

    @property
    def sql(self):
        return "{scheme}://{host}:{port}/sql".format(
            scheme=self.scheme,
            host=self.host,
            port=self.port,
        )
    @property
    def rpc(self):
        return "{scheme}://{host}:{port}/rpc".format(
            scheme=self.scheme,
            host=self.host,
            port=self.port,
        )
