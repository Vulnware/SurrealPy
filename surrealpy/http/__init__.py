import sys
from typing import Optional, Union
import requests
from requests.auth import HTTPBasicAuth
import logging
import urllib.parse
from surrealpy.http.models import SurrealResponse
import aiohttp
from aiohttp import BasicAuth
from surrealpy.exceptions import ConnectionError, SurrealError
from surrealpy.utils import json_dumps, json_loads


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


class SurrealAsyncClient:
    def __init__(
        self, uri: str, namespace: str, database: str, *, username: str, password: str
    ) -> None:
        self.__session = aiohttp.ClientSession(
            auth=BasicAuth(username, password),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "NS": namespace,
                "DB": database,
            },
        )
        self.__uri = uri
        self.__baseurl = base_url(uri)

        self.__ns = namespace
        self.__db = database

    async def check(self):
        return await self.__check()

    @property
    def ns(self) -> str:
        return self.__ns

    @property
    def db(self) -> Union[dict, list]:
        # retrieve all databases
        return self.__db

    @property
    def tables(self) -> Union[dict, list]:
        # retrieve all tables
        data = self.__query("INFO FOR DB;")
        for i in data[0]["result"]["tb"].keys():
            yield i

    @property
    def uri(self):
        return self.__uri

    async def __check(self):
        async with self.__session.head(self.uri) as resp:
            if resp.status == 200:
                return True
            else:
                return False

    async def __post(self, *extends, query: str) -> Union[dict, list]:
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        async with self.__session.post(extend_uri, data=query) as response:
            respText = await response.text()
            logger.debug(respText)
            if response.status == 200:
                return json_loads(respText)
            elif response.status == 400:
                logger.error(respText)
                raise SurrealError(json_loads(respText)["information"])
            else:
                logger.error(respText)
                return None

    async def __get(self, *extends) -> Union[dict, list]:
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        async with self.__session.get(extend_uri) as response:
            respText = await response.text()
            if response.status == 200:
                return json_loads(respText)
            else:
                return None

    def __transform_into_sql_val(val):
        if isinstance(val, str):
            return "'%s'" % val
        return val

    async def raw_query(self, query: str) -> Union[dict, list]:
        # execute a raw query
        return await self.__post("sql", query=query)

    async def mapped_query(self, query: str) -> SurrealResponse:
        # execute a mapped query

        return SurrealResponse(self.raw_query(query))

    async def query(self, query: str, **kwargs) -> Union[dict, list]:
        # query the database
        return await self.mapped_query(query.format(**kwargs))

    async def let(self, key, val):
        return await self.mapped_query(
            "LET ${}={}".format(key, self.__transform_into_sql_val(val))
        )

    async def select(self, tid: str):
        """tid can be a table name or a record id"""
        return await self.mapped_query("SELECT * FROM {}".format(tid))

    async def create(self, tid: str, data: Optional[dict] = None):
        return (
            await self.mapped_query(f"CREATE {tid} CONTENT {json_dumps(data)}")
            if data is not None
            else self.mapped_query(f"CREATE {tid}")
        )

    async def update(self, tid: str, data: Optional[dict] = None):
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} CONTENT {json_dumps(data)}")
        )

    async def change(self, tid: str, data: Optional[dict] = None):
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} MERGE {json_dumps(data)}")
        )

    async def modify(self, tid: str, data: Optional[dict] = None):
        return (
            await self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} PATCH {json_dumps(data)}")
        )

    async def delete(self, tid: str):
        return await self.mapped_query(f"DELETE * FROM {tid}")


class SurrealClient:
    def __init__(
        self, uri: str, namespace: str, database: str, *, username: str, password: str
    ) -> None:
        self.__session = requests.session()
        self.__session.auth = HTTPBasicAuth(username, password)
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
        if self.__check() is False:
            raise ConnectionError("Connection Error")
        self.__ns = namespace
        self.__db = database

    @property
    def ns(self) -> str:
        return self.__ns

    @property
    def db(self) -> Union[dict, list]:
        # retrieve all databases
        return self.__db

    @property
    def tables(self) -> Union[dict, list]:
        # retrieve all tables
        data = self.raw_query("INFO FOR DB;")
        for i in data[0]["result"]["tb"].keys():
            yield i

    @property
    def uri(self):
        return self.__uri

    def __check(self):
        try:
            with self.__session.head(self.__uri) as response:
                logger.debug(response.status_code)
                logger.debug(response.text)
                logger.debug(response.headers)
                logger.debug(response.request.headers)
                logger.debug(response.request.body)
                return True
        except requests.Timeout as e:
            logger.error(e)
            return False
        except requests.ConnectionError as e:
            logger.error(e)
            return False
        except requests.RequestException as e:
            logger.error(e)
            return False
        except Exception as e:
            logger.error(e)
            return False
        else:
            logger.debug("connection successful")

    def __post(self, *extends, query: str) -> Union[dict, list]:
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        with self.__session.post(extend_uri, data=query) as response:
            logger.debug(response.text)
            if response.status_code == 200:
                return json_loads(response.text)
            elif response.status_code == 400:
                logger.error(response.text)
                raise SurrealError(json_loads(response.text)["information"])
            else:
                logger.error(response.text)
                return None

    def __get(self, *extends) -> Union[dict, list]:
        extend_uri = self.__baseurl + "/" + "/".join(extends)
        with self.__session.get(extend_uri) as response:
            logger.debug(response.text)
            if response.status_code == 200:
                return json_loads(response.text)
            else:
                return None

    def __transform_into_sql_val(val):
        if isinstance(val, str):
            return "'%s'" % val
        return val

    def raw_query(self, query: str) -> Union[dict, list]:
        # execute a raw query
        return self.__post("sql", query=query)

    def mapped_query(self, query: str) -> SurrealResponse:
        # execute a mapped query
        return SurrealResponse(self.raw_query(query))

    def query(self, query: str, **kwargs) -> Union[dict, list]:
        # query the database
        return self.mapped_query(query.format(**kwargs))

    def let(self, key, val):
        return self.mapped_query(
            "LET ${}={}".format(key, self.__transform_into_sql_val(val))
        )

    def select(self, tid: str):
        """tid can be a table name or a record id"""
        return self.mapped_query("SELECT * FROM {}".format(tid))

    def create(self, tid: str, data: Optional[dict] = None):
        return (
            self.mapped_query(f"CREATE {tid} CONTENT {json_dumps(data)}")
            if data is not None
            else self.mapped_query(f"CREATE {tid}")
        )

    def update(self, tid: str, data: Optional[dict] = None):
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} CONTENT {json_dumps(data)}")
        )

    def change(self, tid: str, data: Optional[dict] = None):
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} MERGE {json_dumps(data)}")
        )

    def modify(self, tid: str, data: Optional[dict] = None):
        return (
            self.mapped_query(f"UPDATE {tid}")
            if data is not None
            else self.mapped_query(f"UPDATE {tid} PATCH {json_dumps(data)}")
        )

    def delete(self, tid: str):
        return self.mapped_query(f"DELETE * FROM {tid}")
