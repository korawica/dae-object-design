import ast
from math import ceil
from typing import (
    Optional,
    Union,
    Any,
    Tuple,
)


def check_int(value: str):
    """
    :usage:
        >>> check_int('')
        False
        >>> check_int('-3')
        True
        >>> check_int('-123.4')
        False
        >>> check_int('543')
        True
    """
    if not value:
        return False
    return value[1:].isdecimal() if value[0] in {'-', '+'} else value.isdecimal()


def convert_str_to_bool(value: Optional[str] = None, force_raise: bool = True) -> bool:
    """
    :usage:
        >>> convert_str_to_bool('yes')
        True

        >>> convert_str_to_bool('false')
        False
    """
    if value is None or value == '':
        return False
    elif value.lower() in {"yes", "true", "t", "1", "y", "1.0"}:
        return True
    elif value.lower() in {"no", "false", "f", "0", "n", "0.0"}:
        return False
    if force_raise:
        raise ValueError(f'value {value!r} does not convert to boolean type')
    return False


def convert_str_to_list(value: Optional[str] = None, force_raise: bool = True) -> list:
    """
    :usage:
        >>> convert_str_to_list('["a", "b", "c"]')
        ['a', 'b', 'c']

        >>> convert_str_to_list('["d""]', force_raise=False)
        ['["d""]']

        >>> convert_str_to_list('["d""]')
        Traceback (most recent call last):
        ...
        ValueError: can not convert string value '["d""]' to list object
    """
    if value is None or value == '':
        return []
    if value.startswith('[') and value.endswith(']'):
        try:
            return ast.literal_eval(value)
        except SyntaxError as err:
            if not force_raise:
                return [value]
            raise ValueError(f"can not convert string value {value!r} to list object") from err
    return [value]


def convert_str_to_dict(value: Optional[str] = None, force_raise: bool = True) -> dict:
    """
    :usage:
        >>> convert_str_to_dict('{"a": 1, "b": 2, "c": 3}')
        {'a': 1, 'b': 2, 'c': 3}

        >>> convert_str_to_dict('{"d""}', force_raise=False)
        {1: '{"d""}'}

        >>> convert_str_to_dict('{"d""}')
        Traceback (most recent call last):
        ...
        ValueError: can not convert string value '{"d""}' to dict object
    """
    if value is None or value == '':
        return {}
    if value.startswith('{') and value.endswith('}'):
        try:
            return ast.literal_eval(value)
        except SyntaxError as err:
            if not force_raise:
                return {1: value}
            raise ValueError(f"can not convert string value {value!r} to dict object") from err
    return {1: value}


def convert_str_to_int_or_float(value: Optional[str] = None) -> Union[int, float]:
    """
    :usage:
        >>> convert_str_to_int_or_float('+3')
        3

        >>> convert_str_to_int_or_float('-3.01')
        -3.01
    """
    if value is None or value == '':
        return 0
    try:
        return int(value)
    except ValueError:
        return float(value)


def must_list(value: Optional[Union[str, list]] = None) -> list:
    if value:
        return convert_str_to_list(value) if isinstance(value, str) else value
    return []


def must_bool(value: Optional[Union[str, int, bool]] = None, force_raise: bool = False) -> bool:
    if value:
        return value if isinstance(value, bool) else convert_str_to_bool(str(value), force_raise=force_raise)
    return False


def convert_str(value: str) -> Any:
    """Convert string value to real type.
    :usage:
        >>> convert_str('1245')
        1245
        >>> convert_str('[1, 2, 3]')
        [1, 2, 3]
        >>> convert_str('1245.123')
        '1245.123'
    """
    if value.startswith(('"', "'",)) and value.endswith(('"', "'",)):
        return value.strip('"') if value.startswith('"') else value.strip("'")
    elif value.isdecimal():
        return convert_str_to_int_or_float(value)
    elif value.startswith('[') and value.endswith(']'):
        return convert_str_to_list(value)
    elif value.startswith('{') and value.endswith('}'):
        return convert_str_to_dict(value)
    elif value in {'True', 'False', }:
        return convert_str_to_bool(value)
    return value


def revert_args(*args, **kwargs) -> Tuple[tuple, dict]:
    """Return arguments and key-word arguments."""
    return args, kwargs


def arguments(value: Optional[str]) -> Tuple[tuple, dict]:
    """Convert arguments string to args and kwargs
    :usage:
        >>> arguments("'value', 1, name='demo'")
        (('value', 1), {'name': 'demo'})

    """
    return eval(f"revert_args({value})")


def round_up(number: float, decimals):
    assert isinstance(number, float)
    assert isinstance(decimals, int)
    assert decimals >= 0
    if decimals == 0:
        return ceil(number)
    factor = 10 ** decimals
    return ceil(number * factor) / factor


def remove_pad(value: str) -> str:
    """Remove zero padding of string
    :usage:
        >>> remove_pad('000')
        '0'

        >>> remove_pad('0123')
        '123'
    """
    return _last_char if (_last_char := value[-1]) == '0' else value.lstrip('0')


if __name__ == '__main__':
    print(eval("revert_args('value', 1, name='demo', _dict={'k1': 'v1', 'k2': 'v2'})"))
