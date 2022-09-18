from typing import Optional
from pydantic import BaseModel
class Result(BaseModel):
    time: str
    status: str
    result: list[Optional[dict]]
class SurrealResponse(BaseModel):
    results: list[Result]