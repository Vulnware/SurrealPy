import pprint
import typing
from termcolor import cprint, colored, COLORS


__all__ = (
    "json_dumps",
    "json_loads",
    "cpprint",
    "cprint",
    "pprint",
    "colored",
    "COLORS",
)

try:
    import orjson

    def json_dumps(obj: typing.Any) -> str:
        # this is a wrapper for orjson.dumps to make it compatible with json.dumps
        return orjson.dumps(obj).decode("utf-8")

    json_loads = orjson.loads
except ImportError:
    import json

    json_dumps = json.dumps
    json_loads = json.loads


def cpprint(
    obj: object,
    color: typing.Optional[str] = None,
    on_color: typing.Optional[str] = None,
    attrs: typing.Optional[typing.List[str]] = None,
    file: typing.Optional[typing.TextIO] = None,
    **kwargs: typing.Optional[typing.Any]
) -> None:
    """
    This is a wrapper for cprint and pprint. This will print the object with the color.

    Parameters
    ----------
    obj: Any
        The object to print. This can be a multiple objects
    color: typing.Optional[str] (default: None)
        The color of the text to print. This can be any color that termcolor supports.
    on_color: typing.Optional[str] (default: None)
        The color of the background to print. This is only used if color is not None.
    attrs: typing.Optional[typing.List[str]] (default: None)
        The attributes of the text to print. This can be any attribute that termcolor supports.
    file: typing.Optional[typing.TextIO] (default: None)
        The file to print to. This is only used if color is not None. Also can be used to redirect the output sys.stdout or sys.stderr
    **kwargs: typing.Optional[typing.Any]
        The keyword arguments to pass to cprint. This can be any keyword argument that termcolor supports

    Returns
    -------
    None

    Raises
    ------
    TypeError
        If the color is not a string.
    ValueError
        If the color is not a valid color that termcolor supports.
    """
    cprint(pprint.pformat(obj), color, on_color, attrs, file=file, **kwargs)
