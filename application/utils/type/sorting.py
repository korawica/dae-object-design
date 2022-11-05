import re
from typing import Iterable, List, Optional, Union, Any
from collections import defaultdict
from functools import partial


def ordered(obj: Any):
    """Order an object."""
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    return obj


def sort_list_by_priority(
        values: Union[Iterable, List],
        priority: List,
        reverse: bool = False,
        mode: Optional[str] = None
) -> List:
    """Sorts an iterable according to a list of priority items.
    :usage:
        >>> sort_list_by_priority(values=[1, 2, 2, 3], priority=[2, 3, 1])
        [2, 2, 3, 1]

        >>> sort_list_by_priority(values={1, 2, 3}, priority=[2,3])
        [2, 3, 1]
    """
    _mode: str = mode or 'default'

    def _enumerate(_values, _priority, _reverse):
        priority_dict = {k: i for i, k in enumerate(_priority)}

        def priority_getter(value):
            return priority_dict.get(value, len(_values))

        return sorted(_values, key=priority_getter, reverse=_reverse)

    def default(_values, _priority, _reverse):
        priority_dict = defaultdict(
            lambda: len(_priority), zip(_priority, range(len(_priority)), ),
        )
        priority_getter = priority_dict.__getitem__  # dict.get(key)
        return sorted(_values, key=priority_getter, reverse=_reverse)

    switcher: dict = {
        'chain': partial(default, values, priority, reverse),
        'enumerate': partial(_enumerate, values, priority, reverse),
    }

    func = switcher.get(_mode, lambda: [])
    return func()


def atoi(text):
    return int(text) if text.isdigit() else text


def alphanumeric_sort(text):
    """
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    """
    return [atoi(c) for c in re.split(r"(\d+)", text)]


def reverse_non_unique_mapping(d):
    result = {}
    for k, v in d.items():
        if v in result:
            result[v].append(k)
        else:
            result[v] = [k]
    return result


def reverse_mapping(d):
    return {v: k for k, v in d.items()}
