import sys
import importlib
from numbers import Number
from collections import deque
from collections.abc import Set, Mapping
from typing import (
    Optional,
    Tuple,
    Any
)
from application.utils.type import split_default

ZERO_DEPTH_BASES = (str, bytes, Number, range, bytearray)


def cached_import(module_path, class_name):
    modules = sys.modules
    if module_path not in modules or (
            # Module is not fully initialized.
            getattr(modules[module_path], '__spec__', None) is not None and
            getattr(modules[module_path].__spec__, '_initializing', False) is True
    ):
        importlib.import_module(module_path)
    return getattr(modules[module_path], class_name)


def import_string(dotted_path):
    """Import a dotted module path and return the attribute/class designated by
    the last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError(f"{dotted_path} doesn't look like a module path") from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(
            f'Module "{module_path}" does not define a "{class_name}" attribute/class'
        ) from err


def only_one(check_list: list, match_list: list, default: bool = True) -> Optional[str]:
    """Get only one element in check list that exists in match list
    :usage:
        >>> only_one(['a', 'b'], ['a', 'b', 'c'])
        'a'
        >>> only_one(['a', 'b'], ['c', 'e', 'f'])
        'c'
        >>> only_one(['a', 'b'], ['c', 'e', 'f'], default=False)

    """
    if len((exist := set(check_list).intersection(set(match_list)))) == 1:
        return list(exist)[0]
    return next((_ for _ in match_list if _ in check_list), (match_list[0] if default else None))


def hasdot(search: str, content: dict) -> bool:
    """
    :usage:
        >>> hasdot('data.value', {'data': {'value': 2}})
        True

        >>> hasdot('data.value.key', {'data': {'value': 2}})
        False

        >>> hasdot('item.value.key', {'data': {'value': 2}})
        False
    """
    _search, _else = split_default(search, '.', maxsplit=1)
    if _search in content and isinstance(content, dict):
        if not _else:
            return True
        if isinstance((result := content[_search]), dict):
            return hasdot(_else, result)
    return False


def getdot(search: str, content: dict, *args, **kwargs) -> Any:
    """
    :usage:
        >>> getdot('data.value', {'data': {'value': 1}})
        1

        >>> getdot('data', {'data': 'test'})
        'test'

        >>> getdot('data.value', {'data': 'test'})
        Traceback (most recent call last):
        ...
        ValueError: 'value' does not exists in test

        >>> getdot('data.value', {'data': {'key': 1}}, None)

        >>> getdot('data.value.getter', {'data': {'value': {'getter': 'success', 'put': 'fail'}}})
        'success'

    """
    _ignore: bool = kwargs.get('ignore', False)
    _search, _else = split_default(search, '.', maxsplit=1)
    if _search in content and isinstance(content, dict):
        if not _else:
            return content[_search]
        if isinstance((result := content[_search]), dict):
            return getdot(_else, result, *args, **kwargs)
        if _ignore:
            return None
        raise ValueError(
            f'{_else!r} does not exists in {result}'
        )
    if args:
        return args[0]
    elif _ignore:
        return None
    raise ValueError(
        f'{_search} does not exists in {content}'
    )


def setdot(search: str, content: dict, value: Any, **kwargs) -> dict:
    """
    :usage:
        >>> setdot('data.value', {'data': {'value': 1}}, 2)
        {'data': {'value': 2}}

        >>> setdot('data.value.key', {'data': {'value': 1}}, 2, ignore=True)
        {'data': {'value': 1}}
    """
    _ignore: bool = kwargs.get('ignore', False)
    _search, _else = split_default(search, '.', maxsplit=1)
    if _search in content and isinstance(content, dict):
        if not _else:
            content[_search] = value
            return content
        if isinstance((result := content[_search]), dict):
            content[_search] = setdot(_else, result, value, **kwargs)
            return content
        if _ignore:
            return content
        raise ValueError(
            f'{_else!r} does not exists in {result}'
        )
    if _ignore:
        return content
    raise ValueError(
        f'{_search} does not exists in {content}'
    )


def get_lines_count(string):
    # TODO: Windows strings
    count = string.count("\n")
    if string[-1] != "\n":
        count += 1
    return count


def getsize(obj_0) -> int:
    """Recursively iterate to sum size of object & members.

        Empty
        Bytes  type        scaling notes
        28     int         +4 bytes about every 30 powers of 2
        37     bytes       +1 byte per additional byte
        49     str         +1-4 per additional character (depending on max width)
        48     tuple       +8 per additional item
        64     list        +8 for each additional
        224    set         5th increases to 736; 21nd, 2272; 85th, 8416; 341, 32992
        240    dict        6th increases to 368; 22nd, 1184; 43rd, 2280; 86th, 4704; 171st, 9320
        136    func def    does not include default args and other attrs
        1056   class def   no slots
        56     class inst  has a __dict__ attr, same scaling as dict above
        888    class def   with slots
        16     __slots__   seems to store in mutable tuple-like structure
                           first slot grows to 48, and so on.
    """
    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, ZERO_DEPTH_BASES):
            # bypass remaining control flow and return
            pass
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, 'items'):
            size += sum(inner(k) + inner(v) for k, v in getattr(obj, 'items')())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))
        return size

    return inner(obj_0)
