from enum import Enum
from typing import Any, Optional
from surrealpy.utils import json_dumps
import warnings
# For now
CLIENT = None
def set_client(client):
    """Do Not Use If Not Required. May Cause Exceptions"""
    warnings.warn("Do Not Use If Not Required. May Cause Exceptions. set_client will be deprecated soon")
    global CLIENT
    CLIENT = client

class Query:
    def __init__(self,query: Optional[str] = ""):
        self._query =  query
    @property
    def query(self):
        return self._query
    def __add__(self,other: "Query"):
        if isinstance(other,Query):
            self._query += " "+other.query
        else:
            raise TypeError("Waited for 'Query' but found %s"%type(other).__name__)
        return self
    def __str__(self) -> str:
        return self.query
    def execute(self,client = None):
        return (CLIENT or client).query(self.query)

class Filter:
    __filters = (
        "eq",
        "ne",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "nin"
    )
    __filters_eq = {
        "eq": "=",
        "ne": "!=",
        "gt": ">",
        "gte":">=",
        "lt": "<",
        "lte":"<=",
        "in":"in",
        "nin":"not in"
    }
    __filters_check = {
        "eq": lambda x: True,
        "ne": lambda x: True,
        "gt": lambda x: True,
        "gte":lambda x: True,
        "lt": lambda x: True,
        "lte":lambda x: True,
        "in":lambda x: isinstance(x,(tuple,set,list)),
        "nin":lambda x: isinstance(x,(tuple,set,list))
    }
    def __init__(self,key:str,value):
        self._splited = key.split("__")
        self._filter = "eq"
        if len(self._splited) > 3:
            raise ValueError("Exceed maximum child query")
        if self._splited[-1] in self.__filters:
            self._filter = self._splited.pop()
        if not self.__filters_check[self._filter](value):
            raise TypeError("'%s' is not supported by '%s' filter"%(type(value).__name__,self._filter))
        self._value = json_dumps(value)
        self._key = ".".join(self._splited)
        self._filtered = f"{self._key} {self.__filters_eq[self._filter]} {self._value}"
    def __str__(self) -> str:
        return self._filtered
    

        
            
class OR:
    def __init__(self,**kwargs) -> None:
        if len(kwargs.keys()) > 1:
            raise ValueError("OR can accept only one field query")
        self.key = None
        self.value = None
        for key,value in  kwargs.items():
            self.key,self.value = key,value
            break
        self.query = str(Filter(key,value))

        
    def __or__(self,other:"OR"):
        if isinstance(other,OR):
            self.query+=" OR %s"%self.query
        else:
            raise TypeError("Waited 'OR' but instead got '%s'"%type(other).__name__)
        return self
    def __str__(self):
        return self.query
        
class Table:
    def __init__(self,tid: str):
        self._query = Query()
        self._all_block = ":" in tid
        self.__select = None
        self.__where = None
        self._splitat = None
        self._groupby = None
        self._orderby = None
        self._limit = None
        self._startat = None
        self.__where_block = False
        if type(tid) == str:
            self.tid = tid
        else:
            raise TypeError("Waited for 'str' but instead got '%s'"%type(self.tid).__name_)
    def execute(self,client = None):
        return self._query.execute(client)
    def _block_wh(self):
        self.__where_block = True
    def select(self,*fields):
        if self.__select is not None:
            return self
        fields_str = ", ".join(fields) if len(fields) else "*"
        self.__select = Query("SELECT %s FROM %s"%(fields_str,self.tid))
        self._query += self.__select
        return self
    def where(self,*options: tuple[OR],**kwargs: dict[str,Any]):
        if self.__where_block:
            raise Exception("Where is blocked")
        elif self.__select is None:
            self.select()
        filters = []
        for key,value in kwargs.items():
            filters.append(str(Filter(key,value)))
        query = "WHERE "
        query += " and ".join(filters)
        if len(options):
            options_query = " OR ".join([str(o) for o in options])
            query += " OR %s"%options_query
        
        self.__where = Query(query)
        self._query += self.__where
        return self
    def split(self,field:str):
        self.select()
        self._query+=Query("SPLIT %s"%field)
        self._block_wh()
        return self
    def groupby(self,*fields):
        self.select()
        self._query += Query("GROUP BY %s"%(", ".join(fields)))
        self._block_wh()
        return self
    class Order(Enum):
        ASC="ASC"
        DESC="DESC"
    def orderby(self,**fields: "Table.Order"):
        self.select()
        if len(fields.keys()) == 0:
            order = "RAND()"
        orders = [f"{field.replace('__','.')} {item.value}" for field,item in fields.items()]
        order = ", ".join(orders) or order
        self._query+=Query(f"ORDER BY {order}")
        self._block_wh()
        return self
    def limit(self,limit,startAt: Optional[int]=None):
        self.select()
        self._block_wh()
        self._query += Query(f"LIMIT {limit} {'START %d'%startAt if startAt is not None else ''}")
        return self
    
    def __repr__(self) -> str:
        return f"Query: {self._query}"