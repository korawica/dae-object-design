"""
This is then main function for open any files in local or remote space
with the best python libraries and the best practice such as build-in
fileopen, mmap, etc.
"""

import abc
import io
import os
import re
import mmap
import yaml
import csv
import json
import pickle
import marshal
# TODO: import msgpack
try:
    from yaml import (
        CSafeLoader as SafeLoader,
        CBaseLoader as BaseLoader,
    )
except ImportError:
    from yaml import (
        SafeLoader,
        BaseLoader,
    )
from typing import (
    Callable,
    Optional,
    Union,
    Any,
    AnyStr,
    IO,
    NoReturn,
)


class FilePlugIn(abc.ABC):

    def load(self) -> Union[dict, list]: ...

    def save(self): ...


class OpenFile:
    """Open File object for open any compression type."""

    compress_support: set = {
        'gzip',
        'gz',
        'xz',
        'bz2',
    }

    def __init__(
            self,
            path: str,
            mode: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = False,
            compress: Optional[str] = None,
            **kwargs
    ):
        """Main initialize of open file object."""
        self.path: str = path
        self.mode: str = mode
        self.encoding: str = encoding or None
        self.memory: bool = memory
        self.compress: Optional[str] = compress
        self._kwargs: dict = kwargs or {}
        if self.compress and self.compress not in self.compress_support:
            raise ValueError(
                f'{self.__class__.__name__} does not support compress with {self.compress!r}'
            )

        self._file: Optional[IO] = None

        # Validate mode for dynamic switch properties
        self.byte_mode: bool = ('b' in self.mode)

    @property
    def compressed(self) -> Any:
        if not self.compress:
            return io
        elif self.compress in {'gzip', 'gz'}:
            import gzip
            return gzip
        elif self.compress in {'bz2'}:
            import bz2
            return bz2
        elif self.compress in {'xz'}:
            import lzma as xz
            return xz
        elif self.compress in {'zip'}:
            # Note: import zipfile
            ...
        raise NotImplementedError

    @property
    def decompress(self) -> Callable:
        if self.compress:
            return self.compressed.decompress
        raise NotImplementedError(
            'Does not implement decompress method for None compress value.'
        )

    @property
    def properties(self) -> dict:
        if not self.mode:
            raise ValueError('mode value does not set.')
        if not self.compress:
            _mode: dict = {'mode': self.mode}
            return _mode if self.byte_mode else (_mode | {'encoding': self.encoding})
        elif not self.byte_mode and self.compress in {'gzip', 'gz', 'xz', 'bz2', }:
            # Add `t` in open file mode for force with text mode.
            return {'mode': f'{self.mode}t', 'encoding': self.encoding}
        elif self.byte_mode and self.compress in {'gzip', 'gz', 'xz', 'bz2', }:
            return {'mode': self.mode}

    def open(self) -> Union[IO, mmap.mmap]:
        f: IO = self.compressed.open(self.path, **self.properties, **self._kwargs)
        if not self.memory:
            return f
        try:
            _access = (mmap.ACCESS_READ if ('r' in self.mode) else mmap.ACCESS_WRITE)
            return mmap.mmap(
                f.fileno(),
                length=0,
                access=_access,
            )
        except ValueError:
            self.memory: bool = False
            return f

    def close(self) -> NoReturn:
        if not self._file:
            self._file.close()
            self._file: Optional[IO] = None

    def __enter__(self) -> IO:
        if not self._file:
            self._file = self.open()
        return self._file

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._file.closed:
            self._file.close()


class Reader:
    """Reader object"""
    def __init__(
            self,
            path: str,
            mode: Optional[str] = None,
            encoding: Optional[str] = None,
            *,
            memory: bool = False,
            compress: Optional[str] = None
    ):
        self.path: str = path
        self._mode: str = mode or 'r'
        self.encoding: str = encoding or 'utf-8'
        self.memory: bool = memory
        self.compress: Optional[str] = compress
        self._f: Optional[IO] = None

        # Validate stage for dynamic switch
        self.byte_mode: bool = ('b' in self._mode)

    @property
    def compress_obj(self):
        if not self.compress:
            return io
        elif self.compress in {'gzip', 'gz'}:
            import gzip
            return gzip
        elif self.compress in {'bz2'}:
            import bz2
            return bz2
        elif self.compress in {'xz'}:
            import lzma as xz
            return xz
        elif self.compress in {'zip'}:
            # Note: import zipfile
            ...
        elif self.compress in {'h5', 'hdf5', }:
            # Note: import h5py
            ...
        elif self.compress in {'fits', }:
            # Note: import astropy
            ...
        raise NotImplementedError

    @property
    def mode(self):
        return self._mode

    @property
    def properties(self) -> dict:
        if not self.compress:
            _mode: dict = {'mode': self.mode}
            return _mode if self.byte_mode else (_mode | {'encoding': self.encoding})
        # elif self.compress in {'gzip', 'gz', 'xz', }:
        #     return {'mode': 'rb'}
        elif self.compress in {'gzip', 'gz', 'xz', 'bz2', }:
            return {'mode': 'rt', 'encoding': self.encoding}
        elif self.compress in {'zip'}:
            ...
        raise NotImplementedError

    def open(self) -> Union[IO, Any]:
        f: IO = self.compress_obj.open(self.path, **self.properties)
        if not self.memory:
            return f
        try:
            return mmap.mmap(
                f.fileno(),
                length=0,
                access=mmap.ACCESS_READ
            )
        except ValueError:
            # ValueError: cannot mmap an empty file
            self.memory: bool = False
            return f

    def read(self):
        if not self._f:
            self._f = self.open()
        if not self.compress:
            return self._f.read().decode(self.encoding) if self.memory else self._f.read()
        elif self.compress in {'gzip', 'gz', 'xz', 'bz2', }:
            if self.memory:
                return self.compress_obj.decompress(self._f.read()).decode(self.encoding)
            return self._f.read()

    def close(self) -> NoReturn:
        if not self._f:
            self._f.close()
            self._f: Optional[IO] = None

    def __enter__(self) -> Union[IO, Any]:
        if not self._f:
            self._f = self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class Writer:

    def __init__(
            self,
            path: str,
            mode: Optional[str] = None,
            encoding: Optional[str] = None,
            *,
            memory: bool = False,
            compress: Optional[str] = None
    ):
        self.path: str = path
        self._mode: str = mode or 'w'
        self.encoding: str = encoding or 'utf-8'
        self.memory: bool = memory
        self.compress: Optional[str] = compress
        self._f: Optional = None

        # Validate stage for dynamic switch
        self.byte_mode: bool = ('b' in self._mode)

    @property
    def compress_obj(self):
        if not self.compress:
            return io.open
        elif self.compress in {'gzip', 'gz'}:
            import gzip
            return gzip.open
        elif self.compress in {'bz2'}:
            import bz2
            return bz2.open
        elif self.compress in {'xz'}:
            import lzma as xz
            return xz.open
        elif self.compress in {'zip'}:
            # Note: import zipfile
            ...
        raise NotImplementedError

    @property
    def mode(self):
        return self._mode

    @property
    def properties(self) -> dict:
        if not self.compress:
            _mode: dict = {'mode': self.mode}
            return _mode if self.byte_mode else (_mode | {'encoding': self.encoding})
        # elif self.compress in {'gzip', 'gz', }:
        #     return {'mode': 'wb'}
        elif self.compress in {'gzip', 'gz', 'bz2', 'xz', }:
            return {'mode': 'wt', 'encoding': self.encoding}
        elif self.compress in {'zip'}:
            ...
        raise NotImplementedError

    def open(self):
        f: IO = self.compress_obj(self.path, **self.properties)
        if not self.memory:
            return f
        try:
            return mmap.mmap(
                f.fileno(),
                length=0,
                access=mmap.ACCESS_WRITE
            )
        except ValueError:
            # ValueError: cannot mmap an empty file
            return f

    def write(self, data: AnyStr) -> NoReturn:
        if not self._f:
            self._f = self.open()
        self._f.write(data)

    def close(self) -> NoReturn:
        if not self._f:
            self._f.close()
            self._f: Optional = None

    @property
    def closed(self):
        return bool(self._f)

    def __enter__(self):
        self._f = self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# (\\)?(\$)({?([A-Z0-9_]+)}?)
RE_DOTENV_VAR: re.Pattern = re.compile(r"""
    (\\)?               # is it escaped with a backslash?
    (\$)                # literal $
    (                   # collect braces with var for sub
        {?              # allow brace wrapping
        ([A-Z0-9_]+)    # match the variable
        }?              # closing brace
    )                   # braces end
""", re.IGNORECASE | re.VERBOSE)


# ^\s*(?:export\s+)?(?P<name>[\w.-]+)(?:\s*=\s*?|:\s+?)(?P<value>\s*\'(?:\\'|[^'])*\'|\s*\"(?:\\"|[^"])*\"
# |\s*`(?:\\`|[^`])*`|[^#\r\n]+)?\s*$
RE_DOTENV: re.Pattern = re.compile(r"""
    ^\s*(?:export\s+)?      # optional export
    (?P<name>[\w.-]+)       # name of key
    (?:\s*=\s*?|:\s+?)      # separator `=` or `:`
    (?P<value>
        \s*\'(?:\\'|[^'])*\'    # single quoted value
        |
        \s*\"(?:\\"|[^"])*\"    # double quoted value
        |
        \s*`(?:\\`|[^`])*`      # backticks value
        |
        [^#\r\n]+           # unquoted value
    )?\s*                   # optional space
    $
""", re.MULTILINE | re.VERBOSE)


class Env:
    """Env object which mapping search engine
    """

    def __init__(
            self,
            name: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = True
    ):
        self._path: str = name
        self._encoding: str = encoding or 'utf-8'
        self._memory: bool = memory
        self.keep_newline: bool = False
        self.default_value: str = ''

    def load(self, update: bool = True) -> dict:
        with Reader(self._path, encoding=self._encoding, memory=self._memory) as _r:
            _result: dict = self.search_env(_r.read())
            if update:
                os.environ.update(**_result)
            return _result

    def search_env(
            self,
            contents: str,
    ) -> dict:
        """Prepare content data from .env string format before load to the OS environment
        :ref:
            - python-dotenv
              https://github.com/theskumar/python-dotenv
        """
        env: dict = {}
        for content in RE_DOTENV.finditer(contents):
            name: str = content.group('name')
            _value: str = content.group('value').strip()  # Remove leading/trailing whitespace
            if not _value:
                raise ValueError(
                    f'Value {name:!r} in `.env` file does not set value of variable'
                )
            value: str = _value if self.keep_newline else ''.join(_value.splitlines())
            quoted: Optional[str] = None

            # Remove surrounding quotes
            if m2 := re.match(r'^(?P<quoted>[\'\"`])(?P<value>.*)\1$', value):
                quoted: str = m2.group('quoted')
                value: str = m2.group('value')

            if quoted == "'":
                env[name] = value
                continue
            elif quoted == '"':
                # Unescape all chars except $ so variables can be escaped properly
                value: str = re.sub(r'\\([^$])', r'\1', value)

            # Substitute variables in a value
            for sub_content in RE_DOTENV_VAR.findall(value):
                replace: str = ''.join(sub_content[1:-1])
                if sub_content[0] != '\\':
                    # Replace it with the value from the environment
                    replace: str = env.get(sub_content[-1], os.environ.get(sub_content[-1], self.default_value))
                value: str = value.replace(''.join(sub_content[:-1]), replace)
            env[name] = value
        return env


# (\s|^)#.*
RE_YAML_COMMENT: re.Pattern = re.compile(
    r"(\s|^)#.*",
    re.MULTILINE | re.UNICODE | re.IGNORECASE
)


# [\"\']?(\$(?:(?P<escaped>\$|\d+)|({(?P<braced>.*?)(:(?P<braced_default>.*?))?})))[\"\']?
RE_ENV_SEARCH: re.Pattern = re.compile(r"""
    [\"\']?                             # single or double quoted value
    (\$(?:                              # start with non-capturing group
        (?P<escaped>\$|\d+)             # escape $ or number like $1
        |
        ({(?P<braced>.*?)               # value if use braced {}
        (:(?P<braced_default>.*?))?})   # value default with sep :
    ))
    [\"\']?                             # single or double quoted value
""", re.MULTILINE | re.UNICODE | re.IGNORECASE | re.VERBOSE)


def search_env_replace(
        contents: str,
        raise_if_default_not_exists: bool = False,
        default_value: str = 'N/A',
        escape_replaced: str = 'ESC',
) -> str:
    """Prepare content data before parse to any file loading method"""
    shifting: int = 0
    replaces: dict = {}
    replaces_esc: dict = {}
    for content in RE_ENV_SEARCH.finditer(contents):
        search: str = content.group(1)
        if not (escaped := content.group("escaped")):
            variable: str = content.group('braced')
            default: str = content.group('braced_default')
            if not default and raise_if_default_not_exists:
                raise ValueError(
                    f'Could not find default value for {variable} in `.yaml` file'
                )
            elif not variable:
                raise ValueError(
                    f'Value {search!r} in `.yaml` file has something wrong with regular expression'
                )
            replaces[search] = os.environ.get(variable, default) or default_value
        elif '$' in escaped:
            span = content.span()
            search = f'${{{escape_replaced}{escaped}}}'
            contents = (
                    contents[:span[0] + shifting] + search + contents[span[1] + shifting:]
            )
            shifting += len(search) - (span[1] - span[0])
            replaces_esc[search] = '$'
    for _replace in sorted(replaces, reverse=True):
        contents = contents.replace(_replace, replaces[_replace])
    for _replace in sorted(replaces_esc, reverse=True):
        contents = contents.replace(_replace, replaces_esc[_replace])
    return contents


class Yaml:
    """Yaml object
    { Y, true, Yes, ON  }    : Boolean true
    { n, FALSE, No, off }    : Boolean false
    """
    def __init__(
            self,
            name: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = True
    ):
        self._path: str = name
        self._encoding: str = encoding or 'utf-8'
        self._memory: bool = memory

    def load(self) -> dict:
        with Reader(self._path, encoding=self._encoding, memory=self._memory) as _r:
            return yaml.load(_r.read(), SafeLoader)

    def save(self, data) -> NoReturn:
        with Writer(self._path, encoding=self._encoding, memory=self._memory) as _w:
            yaml.dump(data, _w, default_flow_style=False)


class YamlEnv(Yaml):
    """Yaml object which mapping search environment variable engine
    """
    _raise_if_default_not_exists: bool = False
    _default_value: str = 'N/A'
    _escape_replaced: str = 'ESC'

    def load(self) -> dict:
        with Reader(self._path, encoding=self._encoding, memory=self._memory) as _r:
            _debug = _r.read()
            # if _result := yaml.load(self._search_yaml(RE_YAML_COMMENT.sub("", _r.read())), SafeLoader):
            _env_search = self._search_yaml(RE_YAML_COMMENT.sub("", _debug))
            # if _result := yaml.load(_env_search, BaseLoader):
            if _result := yaml.load(_env_search, SafeLoader):
                return _result
            return {}

    def save(self, data) -> NoReturn:
        raise NotImplementedError

    def _search_yaml(
            self,
            contents: str,
    ) -> str:
        """Prepare content data from `.yaml` file before parse to `yaml.load`"""
        return search_env_replace(
            contents,
            raise_if_default_not_exists=self._raise_if_default_not_exists,
            default_value=self._default_value,
            escape_replaced=self._escape_replaced
        )


class CSV:
    def __init__(
            self,
            path: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = False,
    ):
        self._path: str = path
        self._encoding: str = encoding or 'utf-8'
        self._memory: bool = memory

    def load(self, compress: Optional[str] = None) -> list:
        with OpenFile(
                self._path, mode='r', encoding=self._encoding, memory=self._memory, compress=compress
        ) as _r:
            try:
                dialect = csv.Sniffer().sniff(_r.read(128))
                _r.seek(0)
                return list(csv.DictReader(_r, dialect=dialect))
            except csv.Error:
                return []

    def save(
            self,
            data: Union[list, dict],
            *,
            compress: Optional[str] = None,
            properties: Optional[dict] = None,
            mode: Optional[str] = None,
    ) -> NoReturn:
        _mode: str = mode or 'w'
        properties: dict = properties or {}
        assert _mode in {'a', 'w', }, 'save mode must contain only value `a` nor `w`.'
        with OpenFile(
                self._path, mode=_mode, encoding=self._encoding, memory=self._memory,
                compress=compress, newline='',
        ) as _w:
            _has_data: bool = True
            if isinstance(data, dict):
                data: list = [data]
            elif not data:
                data: list = [{}]
                _has_data: bool = False
            if _has_data:
                writer = csv.DictWriter(
                    _w,
                    fieldnames=list(data[0].keys()),
                    lineterminator='\n',
                    **properties
                )
                if _mode == 'w' or not self.has_header:
                    writer.writeheader()
                writer.writerows(data)

    @property
    def has_header(self):
        with OpenFile(
                self._path,
                mode='r',
                encoding=self._encoding,
                memory=self._memory,
        ) as _r:
            try:
                return csv.Sniffer().has_header(_r.read(1))
            except csv.Error:
                return False


class CSVPipeDim(CSV):
    def __init__(
            self,
            path: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = False,
    ):
        super(CSVPipeDim, self).__init__(path, encoding=encoding, memory=memory)
        csv.register_dialect('pipe_delimiter', delimiter='|', quoting=csv.QUOTE_ALL)

    def load(self, compress: Optional[str] = None) -> list:
        with OpenFile(
                self._path,
                mode='r',
                encoding=self._encoding,
                memory=self._memory,
                compress=compress,
        ) as _r:
            try:
                return list(csv.DictReader(_r, delimiter='|', quoting=csv.QUOTE_ALL))
            except csv.Error:
                return []

    def save(
            self,
            data: Union[list, dict],
            *,
            compress: Optional[str] = None,
            properties: Optional[dict] = None,
            mode: Optional[str] = None,
    ) -> NoReturn:
        _mode: str = mode or 'w'
        properties: dict = properties or {}
        assert _mode in {'a', 'w', }, 'save mode must contain only value `a` nor `w`.'
        with OpenFile(
                self._path,
                mode=_mode,
                encoding=self._encoding,
                memory=self._memory,
                compress=compress,
                newline='',
        ) as _w:
            _has_data: bool = True
            if isinstance(data, dict):
                data: list = [data]
            elif not data:
                data: list = [{}]
                _has_data: bool = False
            if _has_data:
                writer = csv.DictWriter(
                    _w,
                    fieldnames=list(data[0].keys()),
                    lineterminator='\n',
                    delimiter='|',
                    quoting=csv.QUOTE_ALL,
                    **properties
                )
                if _mode == 'w' or not self.has_header:
                    writer.writeheader()
                writer.writerows(data)


class Json:
    def __init__(
            self,
            path: str,
            *,
            encoding: Optional[str] = None,
            memory: bool = False,
    ):
        self._path: str = path
        self._encoding: str = encoding or 'utf-8'
        self._memory: bool = memory

    def load(self, compress: Optional[str] = None) -> Union[dict, list]:
        with Reader(self._path, encoding=self._encoding, memory=self._memory, compress=compress) as _r:
            try:
                return json.loads(_r.read())
            except json.decoder.JSONDecodeError:
                return {}

    def save(self, data, *, compress: Optional[str] = None, indent: int = 4) -> NoReturn:
        _w: IO
        with Writer(self._path, encoding=self._encoding, memory=self._memory, compress=compress) as _w:
            if compress:
                _w.write(json.dumps(data))
            else:
                json.dump(data, _w, indent=indent)


class JsonEnv(Json):
    _raise_if_default_not_exists: bool = False
    _default_value: str = 'N/A'
    _escape_replaced: str = 'ESC'

    def load(self, compress: Optional[str] = None) -> dict:
        with Reader(self._path, encoding=self._encoding, memory=self._memory, compress=compress) as _r:
            return json.loads(self._search_json(_r.read()))

    def save(self, data, *, compress: Optional[str] = None, indent: int = 4) -> NoReturn:
        raise NotImplementedError

    def _search_json(
            self,
            contents: str,
    ) -> str:
        """Prepare content data from `.json` file before parse to `json.load`"""
        return search_env_replace(
            contents,
            raise_if_default_not_exists=self._raise_if_default_not_exists,
            default_value=self._default_value,
            escape_replaced=self._escape_replaced
        )


class Pickle:
    def __init__(
            self,
            path,
            *,
            encoding: Optional[str] = None,
    ):
        self._path: str = path
        self._encoding: str = encoding or 'utf-8'

    def load(self):
        with Reader(self._path, mode='rb') as _r:
            return pickle.loads(_r.read())

    def save(self, data):
        _w: IO
        with Writer(self._path, mode='wb') as _w:
            pickle.dump(data, _w)


class Marshal:
    def __init__(
            self,
            path,
            *,
            encoding: Optional[str] = None,
    ):
        self._path: str = path
        self._encoding: str = encoding or 'utf-8'

    def load(self):
        with Reader(self._path, mode='rb') as _r:
            return marshal.loads(_r.read())

    def save(self, data):
        _w: IO
        with Writer(self._path, mode='wb') as _w:
            marshal.dump(data, _w)


__all__ = [
    'Env',
    'Json',
    'JsonEnv',
    'Yaml',
    'YamlEnv',
    'CSV',
    'CSVPipeDim',
    'Marshal',
    'Pickle',
]


def test_read_and_write():
    root: str = r'D:\korawica\Work\dev02_miniproj\GITHUB\dde-object-defined'

    def test_write_normal():
        a = Writer(rf'{root}\data\loader\tests.json', memory=False)
        a.write('Write data with normal mode')
        a.close()

    def test_read_normal():
        with Reader(rf'{root}\data\loader\tests.json', memory=False, encoding='utf-8') as f:
            print(f.read())

    def test_write_gzip():
        a = Writer(rf'{root}\data\loader\tests.gz.json', memory=False, compress='gzip')
        a.write('Write data with gzip mode')
        a.close()

    def test_read_gzip():
        a = Reader(rf'{root}\data\loader\tests.gz.json', memory=False, compress='gzip')
        print(a.read())

    def test_write_xz():
        with Writer(rf'{root}\data\loader\tests.xz.json', memory=False, compress='xz') as a:
            a.write('Write data with xz mode')

        a = Writer(rf'{root}\data\loader\tests.xz.json', memory=False, compress='xz')
        a.write('Write data with xz mode and Writer object')
        a.close()
        assert True, a.closed

    def test_read_xz():
        with Reader(rf'{root}\data\loader\tests.xz.json', memory=True, compress='xz') as a:
            print(a.read())

    def test_write_bz2():
        a = Writer(rf'{root}\data\loader\tests.bz2.json', memory=False, compress='bz2')
        a.write('Write data with bz2 mode')
        a.close()

    def test_read_bz2():
        a = Reader(rf'{root}\data\loader\tests.bz2.json', memory=False, compress='bz2')
        print(a.read())

    test_write_normal()
    test_read_normal()
    test_write_gzip()
    test_read_gzip()
    test_write_xz()
    test_read_xz()
    test_write_bz2()
    test_read_bz2()


def test_open_file():

    _memory: bool = False
    root: str = r'D:\korawica\Work\dev02_miniproj\GITHUB\dde-object-defined'

    def test_write_normal():
        with OpenFile(rf'{root}\data\loader\tests.json', mode='w', memory=_memory, encoding='utf-8') as f:
            f.write('Write data with normal mode')

    def test_read_normal():
        with OpenFile(rf'{root}\data\loader\tests.json', mode='r', memory=_memory, encoding='utf-8') as f:
            print(f.read())

    def test_write_gzip():
        with OpenFile(
                rf'{root}\data\loader\tests.gz.json',
                mode='w',
                memory=_memory,
                encoding='utf-8',
                compress='gzip'
        ) as f:
            f.write('Write data with gzip mode')

    def test_read_gzip():
        with OpenFile(
                rf'{root}\data\loader\tests.gz.json',
                mode='rb',
                memory=_memory,
                encoding='utf-8',
                compress='gzip'
        ) as f:
            print(f.read())

    def test_write_xz():
        with OpenFile(
                rf'{root}\data\loader\tests.xz.json',
                mode='w',
                memory=_memory,
                encoding='utf-8',
                compress='xz'
        ) as f:
            f.write('Write data with xz mode')

    def test_read_xz():
        with OpenFile(
                rf'{root}\data\loader\tests.xz.json',
                mode='r',
                memory=_memory,
                encoding='utf-8',
                compress='xz'
        ) as f:
            print(f.read())

    def test_write_bz2():
        with OpenFile(
                rf'{root}\data\loader\tests.bz2.json',
                mode='w',
                memory=_memory,
                encoding='utf-8',
                compress='bz2'
        ) as f:
            f.write('Write data with bz2 mode')

    def test_read_bz2():
        with OpenFile(
                rf'{root}\data\loader\tests.bz2.json',
                mode='r',
                memory=_memory,
                encoding='utf-8',
                compress='bz2'
        ) as f:
            print(f.read())

    test_write_normal()
    test_read_normal()
    test_write_gzip()
    test_read_gzip()
    test_write_xz()
    test_read_xz()
    test_write_bz2()
    test_read_bz2()


if __name__ == '__main__':
    # test_read_and_write()
    test_open_file()
