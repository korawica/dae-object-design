import contextlib
import copy
from time import time
from functools import wraps
# from ..type import round_up
from application.utils.type import round_up


class memoize:
    """
    :usage:
        >>> @memoize
        ... def fib(n):
        ...     if n in (0, 1):
        ...         return 1
        ...     else:
        ...         return fib(n-1) + fib(n-2)
        >>> for i in range(0, 10):
        ...     fib(i)
        1
        1
        2
        3
        5
        8
        13
        21
        34
        55

    """
    def __init__(self, function):
        self.cache = {}
        self.function = function

    def __call__(self, *args, **kwargs):
        key = str(args) + str(kwargs)
        if key in self.cache:
            return self.cache[key]

        value = self.function(*args, **kwargs)
        self.cache[key] = value
        return value


def memoized_property(func_get):
    """Return a property attribute for new-style classes that only calls its getter on the first
    access. The result is stored and on subsequent accesses is returned, preventing the need to
    call the getter any more.

    :usage:
        >>> class C(object):
        ...     load_name_count = 0
        ...     @memoized_property
        ...     def name(self):
        ...         "name's docstring"
        ...         self.load_name_count += 1
        ...         return "the name"
        >>> c = C()
        >>> c.load_name_count
        0
        >>> c.name
        'the name'
        >>> c.load_name_count
        1
        >>> c.name
        'the name'
        >>> c.load_name_count
        1
    """
    attr_name = '_{0}'.format(func_get.__name__)

    @wraps(func_get)
    def func_get_memoized(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func_get(self))
            # print(attr_name)
        return getattr(self, attr_name)

    return property(func_get_memoized)


def clear_cache(attrs: tuple):
    """Clear or delete attribute value of the class that implement cache.
    :usage:
        >>> class C(object):
        ...     load_name_count = 0
        ...     @memoized_property
        ...     def name(self):
        ...         "name's docstring"
        ...         self.load_name_count += 1
        ...         return "the name"
        ...     @clear_cache(attrs=('_name', ))
        ...     def reset(self):
        ...         return "reset cache"
        >>> c = C()
        >>> c.load_name_count
        0
        >>> c.name
        'the name'
        >>> c.load_name_count
        1
        >>> c.reset()
        'reset cache'
        >>> c.name
        'the name'
        >>> c.load_name_count
        2
        >>> c.name
        'the name'
        >>> c.load_name_count
        2

    """
    def clear_cache_internal(func_get):
        @wraps(func_get)
        def func_clear_cache(self, *args, **kwargs):
            for attr in attrs:
                if hasattr(self, attr):
                    delattr(self, attr)
            return func_get(self, *args, **kwargs)
        return func_clear_cache
    return clear_cache_internal


def deepcopy_params(func):
    """Deep copy function

    :usage:
        >>> @deepcopy_params
        ... def test(a, b, c = None ):
        ...     c = c or {}
        ...     a[1] = 3
        ...     b[2] = 4
        ...     c[3] = 5
        ...     return a, b, c
        >>> aa = {1: 2}
        >>> bb = {2: 3}
        >>> cc = {3: 4}
        >>> test(aa, bb, cc)
        ({1: 3}, {2: 4}, {3: 5})

    """
    def func_get(self, *args, **kwargs):
        return func(
            self,
            *(copy.deepcopy(x) for x in args),
            **{k: copy.deepcopy(v) for k, v in kwargs.items()}
        )
    return func_get


class classproperty:
    """Decorator that converts a method with a single cls argument into a property
    that can be accessed directly from the class.
    """
    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


class ClassPropertyDescriptor(object):
    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, klass=None):
        if klass is None:
            klass = type(obj)
        return self.fget.__get__(obj, klass)()

    def __set__(self, obj, value):
        if not self.fset:
            raise AttributeError("can't set attribute")
        type_ = type(obj)
        return self.fset.__get__(obj, type_)(value)

    def setter(self, func):
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self


def class_property(func):
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)
    return ClassPropertyDescriptor(func)


def timing_decorator(name):
    def timing_internal(func):
        @wraps(func)
        def wrap(*args, **kw):
            print(f"Step '{name}' start", flush=True)
            time_start = time()
            result = func(*args, **kw)
            time_end = time()
            print(
                f"Step '{name}' took: {round_up(time_end - time_start, 2)} sec",
                flush=True,
            )
            return result
        return wrap
    return timing_internal


@contextlib.contextmanager
def measure_performance(title):
    """
    :usage:
        >>> import time
        >>> with measure_performance('test'):
        ...     time.sleep(2)
        test ....................................................... 2.00s
    """
    ts = time()
    yield
    te = time()
    padded_name = "{title} ".format(title=title).ljust(60, ".")
    padded_time = " {:0.2f}".format((te - ts)).rjust(6, ".")
    print(f"{padded_name}{padded_time}s", flush=True)

