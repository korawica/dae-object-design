import json
import yaml
from yaml.representer import (
    Representer,
    SafeRepresenter,
)
from types import GenericAlias
from collections.abc import Mapping
from typing import (
    Any,
    Optional,
)


class BaseConf:
    """Base Configuration variables object keeping from parameters input
    :usage:
        >>> base = BaseConf(parameters={'conn_file_test': {
        ...     'type': 'CSV',
        ...     'properties': {
        ...         'header': True
        ...     }
        ... }})
        >>> base.conn_file_test.properties
        BaseConf(parameters={'header': True})

        >>> base.conn_file_test.type
        'CSV'

        >>> base.conn_file_test.get('not_exists', 'default')
        'default'

        >>> base.conn_file_test.get['not_exists']
        Traceback (most recent call last):
        ...
        TypeError: 'builtin_function_or_method' object is not subscriptable

        >>> for key in base.conn_file_test.properties: key
        'header'

        >>> for value in base.conn_file_test.properties.values(): value
        True
    """
    __class_getitem__ = classmethod(GenericAlias)
    __slots__ = ('__params', '__dict__', )

    def __init__(self, parameters: dict):
        self.__params: dict = {}
        if parameters:
            self.__dict__.update(**{
                key: value
                for key, value in self.__class__.__dict__.items()
                if '__' not in key and not callable(value)
            })
            self.__dict__.update(**parameters)
            self.__params: dict = parameters
        self.__dict__ = self.__inner_structures()

    def __repr__(self):
        _filter: dict = {
            _[0]: _[1]
            for _ in filter(lambda x: not x[0].startswith("_"), self.__dict__.items())
            if not _[0].startswith('_')
        }
        return f'{self.__class__.__name__}(parameters={_filter})'

    def __str__(self):
        return str(
            {
                key: value
                for key, value in self.__params.items()
                if not key.startswith('_')
            }
        )

    def __inner_structures(self):
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, dict):
                self.__dict__[k] = BaseConf(v)
        return self.__dict__

    def __getattr__(self, item):
        return getattr(self.__params, item)

    def __getitem__(self, item):
        return self.__params[item]

    def __iter__(self):
        return iter(self.__params.keys())

    def copy(self):
        return self.__class__(parameters=self.__params)

    def to_dict(self):
        return self.__params


class BaseDictConf(dict):
    """Base Dict Configuration variables object keeping from parameters input"""
    __class_getitem__ = classmethod(GenericAlias)

    def __init__(self, *args, **kwargs):
        super(BaseDictConf, self).__init__(*args, **kwargs)

    def to_dict(self):
        return unconverted(self)

    @classmethod
    def from_dict(cls, d):
        return converted(d, cls)

    @property
    def __dict__(self):
        return self.to_dict()

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__, dict.__repr__(self))

    def __getstate__(self):
        """ Implement a serializable interface used for pickling.
        See https://docs.python.org/3.6/library/pickle.html.
        """
        return dict(self.items())

    def __setstate__(self, state):
        """ Implement a serializable interface used for pickling.
        See https://docs.python.org/3.6/library/pickle.html.
        """
        self.clear()
        self.update(state)

    def __getattr__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            try:
                return self[item]
            except KeyError as err:
                raise AttributeError(item) from err

    def __setattr__(self, key, value):
        self.__getattr__(key)
        object.__setattr__(self, key, value)

    def __delattr__(self, item):
        self.__getattr__(item)
        object.__delattr__(self, item)

    def __dir__(self):
        return list(self.keys())

    def copy(self):
        return type(self).from_dict(self)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def get(self, k, d=None):
        return d if k not in self else self[k]

    def setdefault(self, k, d=None):
        if k not in self:
            self[k] = d
        return self[k]


class AutoBaseDictConf(BaseConf):
    def __setattr__(self, k, v):
        """Works the same as BaseDictConf.__setattr__ but if you supply
        a dictionary as value it will convert it to another BaseDictConf.
        """
        if isinstance(v, Mapping) and not isinstance(v, (AutoBaseDictConf, BaseConf)):
            v = converted(v, AutoBaseDictConf)
        super(AutoBaseDictConf, self).__setattr__(k, v)


class DefaultBaseDictConf(BaseConf):
    def __init__(self, *args, **kwargs):
        """Construct a new DefaultBaseConf. Like collections.defaultdict, the
        first argument is the default value; subsequent arguments are the
        same as those for dict.
        """
        if args:
            default = args[0]
            args = args[1:]
        else:
            default = None
        super(DefaultBaseDictConf, self).__init__(*args, **kwargs)
        self.__default__ = default

    def __getattr__(self, k):
        """ Gets key if it exists, otherwise returns the default value."""
        try:
            return super(DefaultBaseDictConf, self).__getattr__(k)
        except AttributeError:
            return self.__default__

    def __setattr__(self, k, v):
        if k == '__default__':
            object.__setattr__(self, k, v)
        else:
            super(DefaultBaseDictConf, self).__setattr__(k, v)

    def __getitem__(self, k):
        """ Gets key if it exists, otherwise returns the default value."""
        try:
            return super(DefaultBaseDictConf, self).__getitem__(k)
        except KeyError:
            return self.__default__

    def __getstate__(self):
        return self.__default__, dict(self.items())

    def __setstate__(self, state):
        self.clear()
        default, state_dict = state
        self.update(state_dict)
        self.__default__ = default

    @classmethod
    def from_dict(cls, d, default: Optional = None):
        return converted(d, factory=lambda d_: cls(default, d_))

    def copy(self):
        return type(self).from_dict(self, default=self.__default__)

    def __repr__(self):
        return '{0}({1!r}, {2})'.format(type(self).__name__, self.__undefined__, dict.__repr__(self))


class DefaultFactoryBaseDictConf(BaseConf):
    """
        >>> b = DefaultFactoryBaseDictConf(list, {'hello': 'world!'})
        >>> b.hello
        'world!'
        >>> b.foo
        []
        >>> b.bar.append('hello')
        >>> b.bar
        ['hello']
    """

    def __init__(self, default_factory, *args, **kwargs):
        super(DefaultFactoryBaseDictConf, self).__init__(*args, **kwargs)
        self.default_factory = default_factory

    @classmethod
    def from_dict(cls, d, default_factory: Optional = None):
        return converted(d, factory=lambda d_: cls(default_factory, d_))

    def copy(self):
        return type(self).from_dict(self, default_factory=self.default_factory)

    def __repr__(self):
        factory = self.default_factory.__name__
        return '{0}({1}, {2})'.format(type(self).__name__, factory, dict.__repr__(self))

    def __setattr__(self, k, v):
        if k == 'default_factory':
            object.__setattr__(self, k, v)
        else:
            super(DefaultFactoryBaseDictConf, self).__setattr__(k, v)

    def __missing__(self, k):
        self[k] = self.default_factory()
        return self[k]


class RecursiveBaseDictConf(DefaultFactoryBaseDictConf):
    """A BaseDictConf that calls an instance of itself to generate values for
    missing keys.
    :usage:
        >>> b = RecursiveBaseDictConf({'hello': 'world!'})
        >>> b.hello
        'world!'
        >>> b.foo
        RecursiveBaseDictConf(RecursiveBaseDictConf, {})
        >>> b.bar.okay = 'hello'
        >>> b.bar
        RecursiveBaseDictConf(RecursiveBaseDictConf, {'okay': 'hello'})
        >>> b
        RecursiveBaseDictConf(RecursiveBaseDictConf, {'hello': 'world!',
        'foo': RecursiveBaseDictConf(RecursiveBaseDictConf, {}),
        'bar': RecursiveBaseDictConf(RecursiveBaseDictConf, {'okay': 'hello'})})
    """

    def __init__(self, *args, **kwargs):
        super(RecursiveBaseDictConf, self).__init__(RecursiveBaseDictConf, *args, **kwargs)

    @classmethod
    def from_dict(cls, d, default_factory: Optional = None):
        return converted(d, factory=cls)

    def copy(self):
        return type(self).from_dict(self)


def converted(_data, factory: Any = BaseConf):
    seen = dict()

    def cycles(obj):
        try:
            return seen[id(obj)]
        except KeyError:
            pass

        seen[id(obj)] = partial = pre_process(obj)
        return post_process(partial, obj)

    def pre_process(obj):
        if isinstance(obj, Mapping):
            return factory({})
        elif isinstance(obj, list):
            return type(obj)()
        elif isinstance(obj, tuple):
            type_factory = getattr(obj, "_make", type(obj))
            return type_factory(cycles(item) for item in obj)
        else:
            return obj

    def post_process(partial, obj):
        if isinstance(obj, Mapping):
            partial.update((k, cycles(obj[k])) for k in obj.keys())
        elif isinstance(obj, list):
            partial.extend(cycles(item) for item in obj)
        elif isinstance(obj, tuple):
            for (item_partial, item) in zip(partial, obj):
                post_process(item_partial, item)
        return partial

    return cycles(_data)


def unconverted(_data):
    seen = dict()

    def cycles(obj):
        try:
            return seen[id(obj)]
        except KeyError:
            pass

        seen[id(obj)] = partial = pre_process(obj)
        return post_process(partial, obj)

    def pre_process(obj):
        if isinstance(obj, Mapping):
            return dict()
        elif isinstance(obj, list):
            return type(obj)()
        elif isinstance(obj, tuple):
            type_factory = getattr(obj, "_make", type(obj))
            return type_factory(cycles(item) for item in obj)
        else:
            return obj

    def post_process(partial, obj):
        if isinstance(obj, Mapping):
            partial.update((k, cycles(obj[k])) for k in obj.keys())
        elif isinstance(obj, list):
            partial.extend(cycles(v) for v in obj)
        elif isinstance(obj, tuple):
            for (value_partial, value) in zip(partial, obj):
                post_process(value_partial, value)
        return partial

    return cycles(_data)


def to_json(self, **options):
    """ Serializes this BaseDictConf to JSON. Accepts the same keyword options
    as `json.dumps()`.
    :usage:
        >>> b = BaseConf(foo=BaseConf(lol=True), hello=42, ponies='are pretty!')
        >>> json.dumps(b) == b.to_json()
        True
    """
    return json.dumps(self, **options)


def from_json(cls, stream, *args, **kwargs):
    """ Deserializes JSON to BaseConf or any of its subclasses.
    """
    _factory = (lambda d: cls(*(args + (d,)), **kwargs))
    return converted(json.loads(stream), factory=_factory)


BaseDictConf.to_json = to_json
BaseDictConf.from_json = classmethod(from_json)


def _from_yaml(loader, node):
    """PyYAML support for BaseDictConf using the tag `!munch` and `!munch.Munch`.

    :usage:
        >>> import yaml
        >>> yaml.load('''
        ... Flow style: !munch.Munch { Clark: Evans, Brian: Ingerson, Oren: Ben-Kiki }
        ... Block style: !munch
        ...   Clark : Evans
        ...   Brian : Ingerson
        ...   Oren  : Ben-Kiki
        ... ''')  # doctest: +NORMALIZE_WHITESPACE
        {'Flow style': Munch(Brian='Ingerson', Clark='Evans', Oren='Ben-Kiki'),
         'Block style': Munch(Brian='Ingerson', Clark='Evans', Oren='Ben-Kiki')}

        This module registers itself automatically to cover both Munch and any
    subclasses. Should you want to customize the representation of a subclass,
    simply register it with PyYAML yourself.
    """
    data = BaseDictConf()
    yield data
    value = loader.construct_mapping(node)
    data.update(value)


def _to_yaml_safe(dumper, data):
    """ Converts Munch to a normal mapping node, making it appear as a
    dict in the YAML output.

    :usage:
        >>> b = BaseDictConf(foo=['bar', BaseDictConf(lol=True)], hello=42)
        >>> import yaml
        >>> yaml.safe_dump(b, default_flow_style=True)
        '{foo: [bar, {lol: true}], hello: 42}\\n'
    """
    return dumper.represent_dict(data)


def _to_yaml(dumper, data):
    """ Converts Munch to a representation node.

    :usage:
        >>> b = BaseDictConf(foo=['bar', BaseDictConf(lol=True)], hello=42)
        >>> import yaml
        >>> yaml.dump(b, default_flow_style=True)
        '!munch.Munch {foo: [bar, !munch.Munch {lol: true}], hello: 42}\\n'
    """
    return dumper.represent_mapping('!munch.Munch', data)


for loader_name in ("BaseLoader", "FullLoader", "SafeLoader", "Loader", "UnsafeLoader", "DangerLoader"):
    LoaderCls = getattr(yaml, loader_name, None)
    if LoaderCls is None:
        # This code supports both PyYAML 4.x and 5.x versions
        continue
    yaml.add_constructor('!munch', _from_yaml, Loader=LoaderCls)
    yaml.add_constructor('!munch.Munch', _from_yaml, Loader=LoaderCls)

SafeRepresenter.add_representer(BaseConf, _to_yaml_safe)
SafeRepresenter.add_multi_representer(BaseConf, _to_yaml_safe)

Representer.add_representer(BaseConf, _to_yaml)
Representer.add_multi_representer(BaseConf, _to_yaml)


def to_yaml(self, **options):
    """Serializes this Munch to YAML, using `yaml.safe_dump()` if
    no `Dumper` is provided. See the PyYAML documentation for more info.

    :usage:
        >>> b = BaseDictConf(foo=['bar', BaseDictConf(lol=True)], hello=42)
        >>> import yaml
        >>> yaml.safe_dump(b, default_flow_style=True)
        '{foo: [bar, {lol: true}], hello: 42}\\n'
        >>> b.toYAML(default_flow_style=True)
        '{foo: [bar, {lol: true}], hello: 42}\\n'
        >>> yaml.dump(b, default_flow_style=True)
        '!munch.Munch {foo: [bar, !munch.Munch {lol: true}], hello: 42}\\n'
        >>> b.to_yaml(Dumper=yaml.Dumper, default_flow_style=True)
        '!munch.Munch {foo: [bar, !munch.Munch {lol: true}], hello: 42}\\n'
    """
    opts = dict(indent=4, default_flow_style=False)
    opts.update(options)
    if 'Dumper' not in opts:
        return yaml.safe_dump(self, **opts)
    else:
        return yaml.dump(self, **opts)


def from_yaml(cls, stream, *args, **kwargs):
    factory = (lambda d: cls(*(args + (d,)), **kwargs))
    loader_class = kwargs.pop('Loader', yaml.FullLoader)
    return converted(yaml.load(stream, Loader=loader_class), factory=factory)


BaseDictConf.to_yaml = to_yaml
BaseDictConf.from_yaml = classmethod(from_yaml)


__all__ = [
    'BaseConf'
]
