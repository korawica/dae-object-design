import re
import math
import pandas as pd
from typing import (
    Optional,
    Dict,
    Union,
)
from collections import (
    ChainMap,
    defaultdict,
)
from functools import (
    partial,
    reduce,
)
from itertools import zip_longest


def zip_equal(*iterables):
    sentinel = object()
    for combo in zip_longest(*iterables, fillvalue=sentinel):
        if sentinel in combo:
            raise ValueError('Iterables have different lengths')
        yield combo


def merge_dict(*dicts, **kwargs) -> dict:
    """Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.

    :rtype: object
    :usage:
        >>> merge_dict({1: 'one',2: 'two',3: 'three'}, {3: 'Three',4: 'Four'})
        {1: 'one', 2: 'two', 3: 'Three', 4: 'Four'}

    :ref:
        - Performance compare with another functions
          https://gist.github.com/treyhunner/f35292e676efa0be1728"""
    _mode: str = kwargs.pop('mode', 'chain')

    def chain_map(*_dicts) -> dict:
        """:performance: 1
        """
        return dict(ChainMap({}, *reversed(_dicts)))

    def update_map(*_dicts) -> dict:
        """:performance: 2
        """
        result: dict = {}
        for _dict in _dicts:
            result.update(_dict)
        return result

    def reduce_map(*_dicts):
        """:performance: 3
        """
        return reduce(lambda x, y: dict(x, **y), _dicts)

    switcher: dict = {
        'chain': partial(chain_map, *dicts),
        'update': partial(update_map, *dicts),
        'reduce': partial(reduce_map, *dicts),
    }

    func = switcher.get(_mode, lambda: {})
    return func()


def merge_list(*lists, **kwargs) -> list:
    """
    :usage:
        >>> merge_list(['A', 'B', 'C'], ['C', 'D'])
        ['A', 'B', 'C', 'C', 'D']

    """
    _mode: str = kwargs.pop('mode', 'extend')

    def extend_list(*_lists):
        result: list = []
        for _list in _lists:
            result.extend(_list)
        return result

    def reduce_list(*_lists):
        return reduce(lambda x, y: x + y, _lists)

    switcher: dict = {
        'extend': partial(extend_list, *lists),
        'reduce': partial(reduce_list, *lists),
    }

    func = switcher.get(_mode, lambda: [])
    return func()


def merge_dict_value(*dicts, **kwargs) -> dict:
    """
    :usage:
        >>> merge_dict_value({'a': 1, 'b': 5}, {'a': 3, 'b': 6})
        {'a': [1, 3], 'b': [5, 6]}
    """
    _dup: bool = kwargs.pop('duplicate', True)
    _mode: str = kwargs.pop('mode', 'default')

    def default_map(dup: bool, *_dicts) -> dict:
        super_dict: defaultdict = defaultdict(list if dup else set)
        for _dict in _dicts:
            for k, v in _dict.items():
                if dup:
                    super_dict[k].append(v)
                else:
                    super_dict[k].add(v)
        return dict(super_dict)

    switcher: dict = {
        'default': partial(default_map, _dup, *dicts),
    }

    func = switcher.get(_mode, lambda: {})
    return func()


def merge_dict_values(*dicts, **kwargs) -> dict:
    """
    :usage:
        >>> merge_dict_values({'a': [1, 2], 'b': []}, {'a': [1, 3], 'b': [5, 6]})
        {'a': [1, 2, 1, 3], 'b': [5, 6]}
    """
    _dup: bool = kwargs.pop('duplicate', True)

    super_dict = defaultdict(list)
    for _dict in dicts:
        for k, v in _dict.items():
            super_dict[k] = list(super_dict[k] + v) if _dup else list(set(super_dict[k] + v))
    return dict(super_dict)


def merge_values(
        _dict: Dict[int, Union[int, float]],
        start: Optional[int] = None,
        end: Optional[int] = None
) -> Union[int, float]:
    """
    :usage:
        >>> merge_values({1: 128, 2: 134, 3: 45, 4: 104, 5: 129}, start=3, end=5)
        278
    """
    _start: int = start or 0
    _end: int = (end or len(_dict)) + 1
    return sum(map(_dict.get, range(_start, _end)))


def split_str(strings, sep: str = r"\s+"):
    """
    warning: does not yet work if sep is a lookahead like `(?=b)`
    usage:
        >> split_str('.......A...b...c....', sep='...')
        <generator object split_str.<locals>.<genexpr> at 0x7fe8530fb5e8>

        >> list(split_str('A,b,c.', sep=','))
        ['A', 'b', 'c.']

        >> list(split_str(',,A,b,c.,', sep=','))
        ['', '', 'A', 'b', 'c.', '']

        >> list(split_str('.......A...b...c....', '...'))
        ['', '', '.A', 'b', 'c', '.']

        >> list(split_str('   A  b  c. '))
        ['', 'A', 'b', 'c.', '']
    """
    if not sep:
        return iter(strings)
    # return (_.group(1) for _ in re.finditer(f'(?:^|{sep})((?:(?!{sep}).)*)', string))
    # alternatively, more verbosely:
    regex = f'(?:^|{sep})((?:(?!{sep}).)*)'
    for match in re.finditer(regex, strings):
        yield match.group(1)


def isplit(source, sep=None, regex=False):
    """generator version of str.split()
    :param source: source string (unicode or bytes)
    :param sep: separator to split on.
    :param regex: if True, will treat sep as regular expression.
    :returns:
        generator yielding elements of string.

    usage:
        >> print list(isplit("abcb","b"))
        ['a','c','']
    """
    if sep is None:
        # mimic default python behavior
        source = source.strip()
        sep = "\\s+"
        if isinstance(source, bytes):
            sep = sep.encode("ascii")
        regex = True
    start = 0
    if regex:
        # version using re.finditer()
        if not hasattr(sep, "finditer"):
            sep = re.compile(sep)
        for m in sep.finditer(source):
            idx = m.start()
            assert idx >= start
            yield source[start:idx]
            start = m.end()
        yield source[start:]
    else:
        # version using str.find(), less overhead than re.finditer()
        sep_size = len(sep)
        while True:
            idx = source.find(sep, start)
            if idx == -1:
                yield source[start:]
                return
            yield source[start:idx]
            start = idx + sep_size


def split_iterable(iterable, chunk_size=None, generator_flag: bool = True):
    """
    Split an iterable into mini batch with batch length of batch_number
    supports batch of a pandas dataframe
    usage:
        >> for i in split_iterable([1,2,3,4,5], chunk_size=2):
        >>    print(i)
        [1, 2]
        [3, 4]
        [5]

        for idx, mini_data in split_iterable(batch(df, chunk_size=10)):
            print(idx)
            print(mini_data)
    """
    chunk_size: int = chunk_size or 25000
    num_chunks = math.ceil(len(iterable) / chunk_size)
    if generator_flag:
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                yield iterable.iloc[_ * chunk_size:(_ + 1) * chunk_size]
            else:
                yield iterable[_ * chunk_size:(_ + 1) * chunk_size]
    else:
        _chunks: list = []
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                _chunks.append(iterable.iloc[_ * chunk_size:(_ + 1) * chunk_size])
            else:
                _chunks.append(iterable[_ * chunk_size:(_ + 1) * chunk_size])
        return _chunks


def split_default(source: str, sep: str = None, maxsplit: int = -1):
    """
    :usage:
        >>> split_default('asd|fasd', '|', maxsplit=2)
        ['asd', 'fasd', None]
        >>> split_default('data', '.', maxsplit=1)
        ['data', None]
    """
    if maxsplit == -1:
        return source.split(sep, maxsplit)
    _old: list = source.split(sep, maxsplit)
    _result: list = [None] * ((maxsplit + 1) - len(_old))
    _old.extend(_result)
    return _old


def rsplit_default(source: str, sep: str = None, maxsplit: int = -1):
    """
    :usage:
        >>> rsplit_default('asd|fasd', '|', maxsplit=2)
        [None, 'asd', 'fasd']
    """
    if maxsplit == -1:
        return source.rsplit(sep, maxsplit)
    _old: list = source.rsplit(sep, maxsplit)
    _result: list = [None] * ((maxsplit + 1) - len(_old))
    _result.extend(_old)
    return _result


def chunks(dataframe: pd.DataFrame, n: int):
    """Yield successive n-sized chunks from dataframe.
    """
    for i in range(0, len(dataframe), n):
        yield dataframe.iloc[i:i+n]
