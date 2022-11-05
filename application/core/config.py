# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import abc
import re
import os
import json
import inspect
import logging
import shutil
import sqlite3
import contextlib
import dataclasses
from functools import wraps
from urllib.parse import urlparse
from datetime import datetime
from sqlite3 import Error
from typing import (
    List,
    Dict,
    Tuple,
    Iterator,
    Optional,
    Union,
    TypeVar,
    Any,
    AnyStr,
    Callable,
)
from application.io import (
    YamlEnv,
    YamlConf,
    EnvConf,
    Json,
    CSVPipeDim,
    Pickle,
    PathSearch,
    join_path,
    remove_file,
)
from application.utils.type import (
    merge_dict,
    arguments,
)
from application.utils.reusables import (
    import_string,
    get_date,
)
from application.errors import ConfigArgumentError


logger = logging.getLogger(__name__)

os.environ['APP_PATH'] = os.getenv('APP_PATH', join_path(os.path.abspath(__file__), '../../..'))

env = EnvConf('.env')

params = YamlConf('conf/parameters.yaml')

FILE_EXTENSION: dict = {
    'json': Json,
    'yaml': YamlEnv,
    'csv': CSVPipeDim,
    'pickle': Pickle,
}


# [\"\']?(?P<search>@secrets{(?P<braced>.*?)(:(?P<braced_default>.*?))?})[\"\']?
# for secrets grouping level.
# [\"\']?(?P<search>@secrets(?P<group>(\.\w+)*)?{(?P<braced>.*?)(:(?P<braced_default>.*?))?})[\"\']?
RE_SECRETS: re.Pattern = re.compile(r"""
    [\"\']?                             # single or double quoted value
    (?P<search>@secrets{                # search string for replacement
        (?P<braced>.*?)                 # value if use braced {}
        (?::(?P<braced_default>.*?))?   # value default with sep :
    })                                  # end with }
    [\"\']?                             # single or double quoted value
""", re.MULTILINE | re.UNICODE | re.IGNORECASE | re.VERBOSE)

# [\"\']?(?P<search>@function{(?P<function>[\w.].*?)(?::(?P<arguments>.*?))?})[\"\']?
RE_FUNCTION: re.Pattern = re.compile(r"""
    [\"\']?                             # single or double quoted value
    (?P<search>@function{               # search string for replacement
        (?P<function>[\w.].*?)          # called function
        (?::(?P<arguments>.*?))?        # arguments for calling function
    })                                  # end with }
    [\"\']?                             # single or double quoted value
""", re.MULTILINE | re.UNICODE | re.IGNORECASE | re.VERBOSE)


def map_secret(value: Any) -> Union[Union[dict, str], Any]:
    """Map the secret value to configuration data."""
    if isinstance(value, dict):
        return {k: map_secret(value[k]) for k in value}
    elif isinstance(value, (list, tuple)):
        return type(value)([map_secret(i) for i in value])
    elif not isinstance(value, str):
        return value
    for search in RE_SECRETS.finditer(value):
        searches: dict = search.groupdict()
        if '.' in (br := searches['braced']):
            raise ConfigArgumentError(
                'secrets', f", value {br!r},  should not contain dot ('.') in get value."
            )
        secrets = YamlConf('conf/secrets.yaml')
        value: str = value.replace(
            searches['search'], secrets.get(br.strip(), searches['braced_default'])
        )
    return value


def map_function(value: Any) -> Union[Union[dict, str], Any]:
    """Map the function result to configuration data."""
    if isinstance(value, dict):
        return {k: map_secret(value[k]) for k in value}
    elif isinstance(value, (list, tuple)):
        return type(value)([map_secret(i) for i in value])
    elif not isinstance(value, str):
        return value
    for search in RE_FUNCTION.finditer(value):
        searches: dict = search.groupdict()
        if not callable((_fn := import_string(searches['function']))):
            raise ConfigArgumentError(
                '@function', f'from function {searches["function"]!r} is not callable.'
            )
        args, kwargs = arguments(searches['arguments'])
        value: str = value.replace(searches['search'], _fn(*args, **kwargs))
    return value


def map_func_to_str(value: Any, fn: Callable[[str], str]) -> Any:
    """Map any function from input argument to configuration data."""
    if isinstance(value, dict):
        return {k: map_func_to_str(value[k], fn) for k in value}
    elif isinstance(value, (list, tuple)):
        return type(value)([map_func_to_str(i, fn) for i in value])
    elif not isinstance(value, str):
        return value
    return fn(value)


class BaseConfFile:
    """Base Config File object for getting data with `.yaml` format and mapping
    environment variables to the content data.
    """
    def __init__(
            self,
            path: str,
            *,
            compress: Optional[str] = None
    ):
        self.path: str = path
        self.compress: Optional[str] = compress
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)

    def load_base(self, name: str, *, order: int = 1) -> dict:
        """Return configuration data from name of the config.

        :param name: str : A name of configuration key that want to search in the path.

        :param order: int : An order number that want to get from ordered list of duplicate
                data. Default 1.
        """
        if _results := [
            merge_dict({'alias': name}, _getter)
            for file in self.files(excluded=['.json'])
            if (_getter := YamlEnv(file).load().get(name))
        ]:
            try:
                # TODO: Validate order number with len of result list before getting.
                return sorted(
                    _results,
                    key=lambda x: (
                        datetime.fromisoformat(x.get('version', '1990-01-01')),
                        len(x),
                    ),
                    reverse=False
                )[-order]
            except IndexError:
                logging.warning(f'Data does not exists in order: -{order} with name: {name!r}.')
        return {}

    def files(
            self,
            name: Optional[str] = None,
            excluded: Optional[list] = None,
    ) -> Iterator:
        """Return all files that exists in the loading path."""
        return filter(lambda x: os.path.isfile(x), PathSearch.from_dict({
            'root': self.path,
            'exclude_name': excluded,
        }).pick(filename=(name or '*')))

    def join(self, name: str) -> AnyStr:
        """Return joined path from name."""
        return join_path(self.path, name)

    def exists(self, filename: str) -> bool:
        """Return true if filename exists in the path."""
        return os.path.exists(self.join(filename))

    def remove(self, filename: str) -> None:
        """Remove with filename."""
        remove_file(self.join(filename))

    def move(self, name: str, destination: str, auto_destination_exists: bool = True):
        """Copy filename to destination path."""
        if auto_destination_exists:
            os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.copy(self.join(name), destination)


class ConfAdapter(abc.ABC):
    """Config Adapter abstract object."""

    def load_stage(self, name: str) -> dict:
        raise NotImplementedError

    def save_stage(self, name: str, data: dict, merge: bool = False) -> None:
        raise NotImplementedError

    def remove_stage(self, name: str, data_name: str) -> None:
        raise NotImplementedError

    def create(self, name: str, **kwargs) -> None:
        raise NotImplementedError


class ConfFile(BaseConfFile, ConfAdapter):
    """Config File Loading Object for get data from configuration and stage
    """
    def __init__(
            self,
            path: str,
            *,
            compress: Optional[str] = None,
            _type: Optional[str] = None,
    ):
        """Main initialize of config file loading object.

        :param path: str : A path of files to action.

        :param compress: Optional[str] : A compress type of action file.

        :param _type: Optional[str] : A extension type of action file.
        """
        super(ConfFile, self).__init__(path, compress=compress)
        self.type: str = _type or 'json'
        if self.type not in FILE_EXTENSION:
            ConfigArgumentError(
                '_type',
                f'file extension: {self.type} does not implement in config loader.',
            )

    def load_stage(self, name: str, default: Optional[Any] = None) -> Union[dict, list]:
        """Return content data from file with filename, default empty dict."""
        try:
            return FILE_EXTENSION[self.type](self.join(name)).load(compress=self.compress)
        except (FileNotFoundError, ):
            return default if (default is not None) else {}

    def save_stage(self, name: str, data: Union[dict, list], merge: bool = False) -> None:
        """Write content data to file with filename. If merge is true, it will load
        current data from file and merge the data content together before write.
        """
        if not merge:
            FILE_EXTENSION[self.type](self.join(name)).save(data, compress=self.compress)
            return
        elif (
                merge and
                'mode' in inspect.getfullargspec(FILE_EXTENSION[self.type].save).annotations
        ):
            FILE_EXTENSION[self.type](self.join(name)).save(data, compress=self.compress, mode='a')
            return

        all_data: Union[dict, list] = self.load_stage(name=name)
        try:
            if isinstance(all_data, list):
                _merge_data: Union[dict, list] = all_data
                if isinstance(data, dict):
                    _merge_data.append(data)
                else:
                    _merge_data.extend(data)
            else:
                _merge_data: Union[dict, list] = merge_dict(all_data, data)
            FILE_EXTENSION[self.type](self.join(name)).save(
                _merge_data,
                compress=self.compress
            )
        except TypeError as err:
            self.remove(filename=name)
            if all_data:
                FILE_EXTENSION[self.type](self.join(name)).save(all_data, compress=self.compress)
            raise err

    def remove_stage(self, name: str, data_name: str):
        """Remove data by name from file with filename."""
        if all_data := self.load_stage(name=name):
            all_data.pop(data_name, None)
            FILE_EXTENSION[self.type](self.join(name)).save(all_data, compress=self.compress)

    def create(self, name: str, initial_data: Optional[Any] = None) -> None:
        """Create filename in path."""
        if not self.exists(filename=name):
            self.save_stage(
                name=name,
                data=({} if initial_data is None else initial_data),
                merge=False
            )


class ConfSQLite(ConfAdapter):
    """Config SQLite Loading Object for get data from stage
    """

    def __init__(self, path: str):
        self.path: str = path
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)

    @contextlib.contextmanager
    def connect(self, database: str):
        """Return SQLite Connection context."""
        _conn = sqlite3.connect(join_path(self.path, database))
        _conn.row_factory = self.dict_factory
        try:
            yield _conn
        except Error as err:
            logger.error(err)
        _conn.close()

    def load_stage(self, name: str) -> dict:
        """Return content data from database with table name, default empty dict."""
        _db, _table = name.rsplit('/', maxsplit=1)
        with self.connect(_db) as conn:
            cur = conn.cursor()
            cur.execute(f"select * from {_table};")
            result = cur.fetchall()
            return (
                {_['conf_name']: self.convert_type(_) for _ in result}
                if result else {}
            )

    def save_stage(self, name: str, data: dict, merge: bool = False) -> None:
        """Write content data to database with table name. If merge is true, it will update
        or insert the data content.
        """
        _db, _table = name.rsplit('/', maxsplit=1)
        _data: dict = self.prepare_values(data.get(list(data.keys())[0]))
        with self.connect(_db) as conn:
            cur = conn.cursor()
            # TODO: change `replace` to on `conflict ( <pk> ) do update set ...`
            query: str = (
                f'insert {"or replace" if merge else ""} into {_table} '
                f'({", ".join(_data.keys())}) values ({(":" + ", :".join(_data.keys()))});'
            )
            cur.execute(query, _data)

    def remove_stage(self, name: str, data_name: str) -> None:
        """Remove data by name from table in database with table name."""
        _db, _table = name.rsplit('/', maxsplit=1)
        with self.connect(_db) as conn:
            cur = conn.cursor()
            query: str = f"delete from {_table} where conf_name = '{data_name}';"
            cur.execute(query)

    def create(self, name: str, schemas: Optional[dict] = None):
        """Create table in database."""
        if not schemas:
            raise ConfigArgumentError(
                'schemas', f'in `create` method of {self.__class__.__name__} should have value.'
            )
        _schemas: str = ', '.join([f'{k} {v}' for k, v in schemas.items()])
        _db, _table = name.rsplit('/', maxsplit=1)
        with self.connect(_db) as conn:
            cur = conn.cursor()
            cur.execute(f"create table if not exists {_table} ({_schemas})")

    @staticmethod
    def dict_factory(cursor, row):
        """Result of dictionary factory.

        :note:
            Another logic of dict factory.

            - dict([(col[0], row[idx]) for idx, col in enumerate(cursor.description)])

            - dict(zip([col[0] for col in cursor.description], row))
        """
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    @staticmethod
    def prepare_values(values: dict) -> dict:
        """Return prepare value with dictionary type to string to source system."""
        results: dict = values.copy()
        for _ in values:
            if isinstance(values[_], dict):
                results[_] = json.dumps(values[_])
        return results

    @staticmethod
    def convert_type(data: dict, key: Optional[str] = None) -> dict:
        """Return converted value from string to dictionary from source system."""
        _key: str = key or 'conf_data'
        _results: dict = data.copy()
        _results[_key] = json.loads(data[_key])
        return _results


ConfLoader = TypeVar('ConfLoader', bound='BaseConfLoader')


class BaseConfLoader:
    """Base Config Loader Object for get data from any config file or table with name."""

    conf_prefix: str = ''

    conf_file_extension: str = ''

    conf_file_initial: Any = {}

    conf_sqlite_schema: dict = {}

    def __init__(
            self,
            endpoint: str,
            name: str,
            *,
            environ: Optional[str] = None
    ):
        """Initialization of base metadata object. The metadata can implement with
        a json file or a table in any database like SQLite.

        :param endpoint: str

        :param name: str

        :param environ: Optional[str]
        """
        _environ: str = f".{_env}" if (_env := (environ or '')) else ''
        url = urlparse(endpoint)
        self._cf_loader_type = url.scheme
        self._cf_loader_endpoint = f'{url.netloc}/{url.path}'
        self._cf_name: str = name

        if self._cf_loader_type == 'file':
            # Case: file : the data in file does not have schemas.
            self._cf_filename: str = f"{self.conf_prefix}{_environ}.{self.conf_file_extension}"
            self.loading: ConfAdapter = ConfFile(
                path=self._cf_loader_endpoint, _type=self.conf_file_extension
            )
            self.loading.create(name=self._cf_filename, initial_data=self.conf_file_initial)
        elif self._cf_loader_type == 'sqlite':
            # Case: sqlite : the data must use `conf_sqlite_schema` for table creation.
            self._cf_filename: str = (
                f"{self.conf_prefix}{_environ}.db/tbl_{self.conf_prefix}"
            )
            self.loading: ConfAdapter = ConfSQLite(path=self._cf_loader_endpoint)
            self.loading.create(
                name=self._cf_filename,
                schemas=self.conf_sqlite_schema
            )
        else:
            raise NotImplementedError(
                f'{self.__class__.__name__} does not support type: {self._cf_loader_type!r}.'
            )

    def load(self) -> dict:
        """Return data from filename."""
        return self.loading.load_stage(name=self._cf_filename).get(self._cf_name, {})

    def save(self, data: Union[dict, list]) -> None:
        """Saving data to source from filename."""
        self.loading.save_stage(name=self._cf_filename, data=data, merge=True)

    def remove(self) -> None:
        """Remove data from filename."""
        self.loading.remove_stage(name=self._cf_filename, data_name=self._cf_name)


class ConfMetadata(BaseConfLoader):
    """Config Metadata Object for get data from metadata file or table
    """

    conf_prefix: str = 'metadata'

    conf_file_extension: str = 'json'

    conf_file_initial: dict = {}

    conf_sqlite_schema: dict = {
        'conf_name': 'varchar(256) primary key',
        'conf_shortname': 'varchar(64) not null',
        'conf_fullname': 'varchar(256) not null',
        'conf_data': 'json not null',
        'update_time': 'datetime not null',
        'register_time': 'datetime not null',
        'author': 'varchar(512) not null',
    }

    def save(self, data: dict) -> None:
        """Saving data to source with name mapping from filename."""
        self.loading.save_stage(name=self._cf_filename, data={self._cf_name: data}, merge=True)


@dataclasses.dataclass()
class Message:
    """Message Data Object for get message string and concat with `||` when pull result.
    """
    _message: str
    _messages: List[str] = dataclasses.field(default_factory=list)

    @property
    def messages(self) -> str:
        return self._message

    @messages.setter
    def messages(self, msg: str):
        self._messages.append(msg)
        self._message += (f'||{msg}' if self._message else f'{msg}')


Logging = TypeVar('Logging', bound='ConfLogging')


def saving(func):
    @wraps(func)
    def wraps_saving(*args, **kwargs):
        self: Logging = args[0]
        _level: str = func.__name__.split('_')[-1].upper()
        if (
                (msg := args[1]) and
                (getattr(logging, _level) >= self._cf_logger.level) and
                (self._cf_auto_save or kwargs.get('force', False))
        ):
            self.save(
                data=self.setup({
                    # TODO: converter msg string before save.
                    'message': msg,
                    'status': _level
                })
            )
        return func(*args, **kwargs)
    return wraps_saving


class ConfLogging(BaseConfLoader):
    """Config Logging Object for log message from any change from register or loader
    process.
    """

    conf_prefix: str = 'logging'

    conf_file_extension: str = 'csv'

    conf_file_initial: list = []

    conf_sqlite_schema: dict = {
        'parent_hash_id': 'varchar(64) not null',
        'hash_id': 'varchar(64) primary key not null',
        'conf_name': 'varchar(256) not null',
        'message': 'text',
        'update_time': 'datetime not null',
        'status': 'varchar(64) not null',
        'author': 'varchar(512) not null',
    }

    def __init__(
            self,
            endpoint: str,
            name: str,
            _logger: logging.Logger,
            *,
            environ: Optional[str] = None,
            setup: Optional[dict] = None,
            auto_save: bool = False
    ):
        super(ConfLogging, self).__init__(endpoint, name, environ=environ)
        self._cf_logger: logging.Logger = _logger
        self._cf_msgs: List[Optional[dict]] = []
        self._cf_parent_hash: str = str(int(get_date('datetime').timestamp()))
        self._cf_setup: dict = merge_dict({
            'conf_name': self._cf_name
        }, (setup or {}))
        self._cf_auto_save: bool = auto_save

    def setup(self, data) -> dict:
        _now = get_date('datetime')
        return merge_dict(
            self._cf_setup,
            {
                'parent_hash_id': self._cf_parent_hash,
                'hash_id': str(int(_now.timestamp())),
                'update_time': _now.strftime('%Y-%m-%d %H:%M:%S')
            },
            data
        )

    def save_logging(self):
        if self.is_pulled:
            self.save(data=self._cf_msgs)
            self._cf_msgs: List[Optional[dict]] = []

    def debug(self, msg):
        self._cf_logger.debug(msg)

    def info(self, msg):
        self._cf_logger.info(msg)

    def warning(self, msg):
        self._cf_logger.warning(msg)

    def critical(self, msg):
        self._cf_logger.critical(msg)

    @saving
    def p_debug(self, msg, force: bool = False):
        if (
                (logging.DEBUG >= self.level) and
                (not self._cf_auto_save) and
                (not force)
        ):
            self._cf_msgs.append(self.setup({'message': msg, 'status': 'DEBUG'}))
        self._cf_logger.debug(msg)

    @saving
    def p_info(self, msg, force: bool = False):
        if (
                logging.INFO >= self.level and
                (not self._cf_auto_save) and
                (not force)
        ):
            self._cf_msgs.append(self.setup({'message': msg, 'status': 'INFO'}))
        self._cf_logger.info(msg)

    @saving
    def p_warning(self, msg, force: bool = False):
        if (
                logging.WARNING >= self.level and
                (not self._cf_auto_save) and
                (not force)
        ):
            self._cf_msgs.append(self.setup({'message': msg, 'status': 'WARNING'}))
        self._cf_logger.warning(msg)

    @saving
    def p_critical(self, msg, force: bool = False):
        if (
                logging.CRITICAL >= self.level and
                (not self._cf_auto_save) and
                (not force)
        ):
            self._cf_msgs.append(self.setup({'message': msg, 'status': 'CRITICAL'}))
        self._cf_logger.critical(msg)

    @property
    def is_pulled(self) -> bool:
        return len(self._cf_msgs) > 0

    @property
    def level(self) -> int:
        return self._cf_logger.level


__all__ = [
    'map_secret',
    'map_function',
    'map_func_to_str',
    'params',
    'env',
    'ConfFile',
    'ConfMetadata',
    'ConfLogging',
    'ConfLoader',
]


def test_meta_sqlite():
    _meta = ConfMetadata(
        'sqlite://D:/korawica/Work/dev02_miniproj/GITHUB/dde-object-defined/data',
        # 'file://D:/korawica/Work/dev02_miniproj/GITHUB/dde-object-defined/data',
        'conn_local_file'
    )
    # _meta.save(
    #     data={
    #         'conf_name': 'conn_local_file',
    #         'conf_shortname': 't',
    #         'conf_fullname': 'super_test',
    #         'conf_data': {
    #             'data': 'ASFGSDFE13123'
    #         },
    #         'update_time': '2022-01-02 00:00:00',
    #         'register_time': '2022-01-01 00:00:00',
    #         'author': 'unknown'
    #     }
    # )
    print(_meta.load())
    print(type(logger))


def test_logging_file():
    _log = ConfLogging(
        'file://D:/korawica/Work/dev02_miniproj/GITHUB/dde-object-defined/data/logs',
        'conn_local_file',
        _logger=logger,
        auto_save=False
    )
    _log.p_info('test log data from info level', force=True)
    _log.p_debug('test log data from debug level1')
    logger.setLevel(logging.INFO)
    _log.p_debug('test log data from debug level2')
    print(_log.is_pulled)


if __name__ == '__main__':
    # test_meta_sqlite()
    test_logging_file()
    # print(os.path.isabs('/test/conf'))
    # print(os.path.exists('/conf'))
