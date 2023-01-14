from typing import  Any, Callable, Union

from surrealpy.builder import Query, Filter, Select, Table, OR
from schema import Schema, And, Use, SchemaError, Optional as SchemaOptional
import logging

logger = logging.getLogger("surrealpy.orm")
# logger will work only if debug is enabled
logger.setLevel(logging.DEBUG)
# add null handler to logger to prevent errors 
logger.addHandler(logging.NullHandler())




class Field:
    def __init__(self,*,default: Union[None,Any] = None,null: bool = False,validator: Union[None,Callable[[Any],bool]] = None):
        self.__name: str = None
        self.__default= default
        self.__connection: Document = None
        self.__null = null
        self.__validator = validator
        # raise error if validator is not callable and not None
        if self.__validator is not None and not callable(self.__validator):
            raise TypeError("Validator must be a callable or None")
    def __str__(self):
        return self.__name
    
    def __getattribute__(self, __name: str) -> Any:
        # if attribute not starts with __ and __connection is not None
        if not __name.startswith("_Field") and (connection:=object.__getattribute__(self,"_Field__connection")) is not None :
            # create a new field with the same attributes of connection's field but add the name of the field to the new field
            new_field = Field(default=object.__getattribute__(self,"_Field__default"),null=object.__getattribute__(self,"_Field__null"),validator=object.__getattribute__(self,"_Field__validator"))
            new_field._Field__name = f"{object.__getattribute__(self,'_Field__name')}.{__name}"
            
            new_field._Field__connection = object.__getattribute__(connection,__name)._Field__connection
            return new_field
        return object.__getattribute__(self,__name)
    def __eq__(self, other: Any) -> Filter:
        return Filter(self.__name,other)
    def __ne__(self, other: Any) -> Filter:
        return Filter(self.__name+"__ne",other)
    def __gt__(self, other: Any) -> Filter:
        return Filter(self.__name+"__gt",other)
    def __ge__(self, other: Any) -> Filter:
        return Filter(self.__name+"__ge",other)
    def __lt__(self, other: Any) -> Filter:
        return Filter(self.__name+"__lt",other)
    def __le__(self, other: Any) -> Filter:
        return Filter(self.__name+"__le",other)
    # in operator
    def __contains__(self, other: Any) -> Filter:
        return Filter(self.__name+"__in",other)
    # not in operator
    def __not_contains__(self, other: Any) -> Filter:
        return Filter(self.__name+"__nin",other)
    def in_(self, other: Any) -> Filter:
        return Filter(self.__name+"__in",other)
    def nin_(self, other: Any) -> Filter:
        return Filter(self.__name+"__nin",other)
    def desc(self) -> str:
        return self.__name + " DESC"
    def asc(self) -> str:
        return self.__name + " ASC"
class Document(Table):
    __schema: Schema = None
    __fields: dict = {}
    def __init__(self,**kwargs):
        # Set tid as the class name 
        
        logger.debug(self.__dict__)
        if val:=kwargs.get("id"):
            kwargs["id"] = f"{self.__class__.__name__}:{val}"
            
        self.__dict__.update(self.__schema.validate(kwargs,ignore_extra_keys=True))
    def dict(self,*,exclude: list = [],include: list = [],ignore_null: bool = True) -> dict:
        # ignore the items that instance of Field
        for key in self.__dict__.copy():
            if isinstance(self.__dict__[key],Field):
                del self.__dict__[key]
        # if exclude is not empty
        if exclude:
            # remove the items that in exclude
            for key in exclude:
                if key in self.__dict__:
                    del self.__dict__[key]
        # if include is not empty
        if include:
            # remove the items that not in include
            for key in self.__dict__.copy():
                if key not in include:
                    del self.__dict__[key]
        # if ignore_null is True
        if ignore_null:
            # remove the items that is None
            for key in self.__dict__.copy():
                if self.__dict__[key] is None:
                    del self.__dict__[key]
        # iterate over the items in __dict__ and if the value is a Document call the dict method of the Document
        for key in self.__dict__.copy():
            if isinstance(self.__dict__[key],Document):
                self.__dict__[key] = self.__dict__[key].dict(exclude=exclude,include=include,ignore_null=ignore_null)
        
        return self.__dict__
    @classmethod
    def select(cls,*fields):
        return Select(cls.__class__.__name__,fields)
    def __init_subclass__(cls) -> None:
        # Add fields to the document
        annotations = cls.__annotations__
        ans = {}
        # search for fields that is nullable
        for key, annotation in annotations.copy().items():
            # get the value from __dict__ if it exists
            if key in cls.__dict__:
                value = cls.__dict__[key]
                # if the value is a Field
                if isinstance(value, Field):
                    
                    # set the name of the field
                    value._Field__name = key
                    # check if field annotation is subclass of a Document if it is set the connection of the field
                    if issubclass(annotation,Document):
                        value._Field__connection = annotation
                    
                    # add the field to the fields dict
                    cls.__fields[key] = value
                    # set the default value of the field
                    if value._Field__null:
                        ans[SchemaOptional(key,default=value._Field__default)] = annotation if value._Field__validator is None else And(annotation,value._Field__validator)
                    
                    else:
                        if value._Field__validator is not None:
                            
                            ans[key] = And(annotation,value._Field__validator)
                        else:
                            ans[key] = annotation
        ans[SchemaOptional("id",default=None)] = str
        cls.__schema = Schema(ans)
        cls.id = Field(default=None,null=True)
        
        return super().__init_subclass__()
    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join([f'{key}={value}' for key,value in self.__dict__.items()])})"
    def __str__(self):
        return f"{self.__class__.__name__}({', '.join([f'{key}={value}' for key,value in self.__dict__.items()])})"

class I(Filter):
    """
    Encapsulate a filter in parentheses
    """
    def __init__(self,filter: Filter):
        self.filter = filter
        self._filtered = f"({self.filter})"
    def __str__(self):
        return self._filtered