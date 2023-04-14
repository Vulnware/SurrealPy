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
    
    def __or__(self,other:"Filter"):
        if isinstance(other,Filter):
            self._filtered = f"{self._filtered} OR {other._filtered}"
            return self
        else:
            raise TypeError("Waited 'Filter' but instead got '%s'"%type(other).__name__)
    def __and__(self,other:"Filter"):
        if isinstance(other,Filter):
            self._filtered = f"{self._filtered} AND {other._filtered}"
            return self
        else:
            raise TypeError("Waited 'Filter' but instead got '%s'"%type(other).__name__)

class Group:
    def __init__(self,*filters:Filter):
        self._filters = filters
    def __str__(self) -> str:
        return " AND ".join([str(f) for f in self._filters])

OR = Filter

class Select:
    def __init__(self,tid:str,fields: tuple[str]):
        self._fields = fields
        self._query = Query("SELECT %s FROM %s"%(", ".join([str(f) for f in fields]) or "*",tid))
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
    @property
    def query(self):
        return self._query
    def execute(self,client = None):
        return self._query.execute(client)
    def _block_wh(self):
        self.__where_block = True
    
    def where(self,*options: tuple[OR],**kwargs: dict[str,Any]):
        if self.__where_block:
            raise Exception("Where is blocked")
        if len(options) == 0 and len(kwargs) == 0:
            raise ValueError("No filter provided")
            
        query = "WHERE "
        queries = []
        last_filter = None
        for key,value in kwargs.items():
            if last_filter is None:
                last_filter = Filter(key,value)
            else:
                last_filter &= Filter(key,value)
        if last_filter is not None:
            queries += [last_filter]
        if len(options):
            for option in options:
                if isinstance(option,Filter):
                    queries += [option]
                else:
                    raise TypeError("Waited for 'Filter' but instead got '%s'"%type(option).__name__)
        query += " AND ".join([str(item) for item in queries])
        self.__where = Query(query)
        self._query += self.__where
        return self
    def split(self,field:str):
        
        self._query+=Query("SPLIT %s"%field)
        self._block_wh()
        return self
    def group_by(self,*fields):
        
        self._query += Query("GROUP BY %s"%(", ".join([str(f) for f in fields])))
        self._block_wh()
        return self
    class Order(Enum):
        ASC="ASC"
        DESC="DESC"
    def order_by(self,*args,**fields: "Table.Order"):
        if len(args) > 0:
            order = ", ".join([str(i) for i in args])
            self._query+=Query(f"ORDER BY {order}")
            self._block_wh()
            return self
        
        
        if len(fields.keys()) == 0:
            order = "RAND()"
        orders = [f"{field.replace('__','.')} {item.value}" for field,item in fields.items()]
        order = ", ".join(orders) or order
        self._query+=Query(f"ORDER BY {order}")
        self._block_wh()
        return self
    def limit(self,limit,startAt: Optional[int]=None):
        
        self._block_wh()
        self._query += Query(f"LIMIT {limit} {'START %d'%startAt if startAt is not None else ''}")
        return self
    def __str__(self) -> str:
        return self._query.query
class Table:
    id: Optional[str] = None
    def __init__(self,id: str):
        self.id = id
    
    def select(self,*fields: str) -> Select:
        return Select(self.id,fields)
    
    def __repr__(self) -> str:
        return f"Query: {self._query}"