from typing import Any, Optional

__all__ = ("json_dumps", "json_loads")

try:
    import orjson

    def json_dumps(obj: Any) -> str:
        # this is a wrapper for orjson.dumps to make it compatible with json.dumps
        return orjson.dumps(obj).decode("utf-8")

    json_loads = orjson.loads
except ImportError:
    import json

    json_dumps = json.dumps
    json_loads = json.loads
