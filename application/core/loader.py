# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import copy
import logging
import pandas as pd
import enum
from functools import cached_property
from typing import (
    Optional,
    List,
    Type,
    Tuple,
    Dict,
    Union,
    TypeVar,
    Any,
)
from application.core.config import (
    map_secret,
    map_function,
    map_func_to_str,
    params,
)
from application.core.register import (
    Register,
    Stage,
)
from application.core.formatter import Datetime
from application.core.objects import ObjectType
from application.core.converter import (
    CronRunner,
    CronJob,
)
from application.utils.type import merge_dict
from application.utils.reusables import (
    import_string,
    clear_cache,
    get_date,
    hasdot,
    getdot,
    setdot,
)
from application.errors import (
    ConfigNotFound,
    ConfigArgumentError,
)

logger = logging.getLogger(__name__)

LoadType = TypeVar('LoadType', bound='BaseLoad')


class BaseLoad:
    """Base configuration data loading object for load config data from `cls.load_stage`
    stage. The base loading object contain necessary properties and method for type object.
    """

    load_prefix: set = set()

    load_stage: str = Stage.final

    load_datetime_name: str = 'audit_date'

    load_datetime_fmt: str = '%Y-%m-%d %H:%M:%S'

    excluded_key: set = {
        'alias',
        'type',
        'update_time',
        'version',
    }

    option_key: set = {
        'parameters',
    }

    datetime_key: set = {
        'endpoint',
    }

    @classmethod
    def from_dict(
            cls,
            name: str,
            content: dict,
            *,
            parameters: Optional[dict] = None
    ) -> LoadType:
        """Manual load configuration data from dict mapping.

        :param name: str : A content name.

        :param content: dict : A manual content data.

        :param parameters: Optional[dict] : ...
        """
        if _alias := content.pop('alias', None):
            name: str = _alias
        return cls(
            data={
                'name': name,
                'fullname': f'manual:{name}',
                'data': content
            },
            parameters=parameters,
            catalog_flag=False
        )

    @classmethod
    def from_catalog(
            cls,
            name: str,
            *,
            refresh: bool = True,
            parameters: Optional[dict] = None
    ) -> LoadType:
        """Catalog load configuration

        :param name: str : A name of configuration data that can contain a domain name.

        :param refresh: bool : A refresh boolean flag for loading data from base and
                auto deploy to `cls.load_stage` again if it set be True.

        :param parameters: Optional[dict] : ...
        """
        if refresh:
            _regis: Register = Register(name=name).deploy(stop=cls.load_stage)
        else:
            try:
                _regis: Register = Register(name=name, stage=cls.load_stage)
            except ConfigNotFound:
                _regis: Register = Register(name=name).deploy(stop=cls.load_stage)
        return cls(
            data={
                'name': _regis.name,
                'fullname': _regis.fullname,
                'data': _regis.data.copy()
            },
            parameters=parameters,
            catalog_flag=True
        )

    def __init__(
            self,
            data: Union[dict, str],
            *,
            refresh: bool = True,
            parameters: Optional[dict] = None,
            catalog_flag: bool = False
    ):
        """Main initialize base config object which get a name of configuration and load
        data by the register object.

        :param data: dict : A configuration data content with fix keys, `name`, `fullname`, and `data`.

        :param parameters: Optional[dict] : A parameters mapping for some sub-class of
                loading use.

        :param catalog_flag: bool : A catalog flag for note in object if method refresh was actioned.
        """
        if isinstance(data, str):
            if refresh:
                _regis: Register = Register(name=data).deploy(stop=self.load_stage)
            else:
                try:
                    _regis: Register = Register(name=data, stage=self.load_stage)
                except ConfigNotFound:
                    _regis: Register = Register(name=data).deploy(stop=self.load_stage)
            data: dict = {
                'name': _regis.name,
                'fullname': _regis.fullname,
                'data': _regis.data.copy()
            }
            catalog_flag: bool = True
        self._ld_regis: dict = data
        self._ld_catalog_flg: bool = catalog_flag
        self._ld_secrets: bool = params.engine.config_loader_secrets
        self._ld_function: bool = params.engine.config_loader_function
        self._parameters: dict = merge_dict({
            self.load_datetime_name: get_date(self.load_datetime_fmt)
        }, (parameters or {}))

        # Validate step of base loading object.
        if not any(self._ld_regis['name'].startswith(prefix) for prefix in self.load_prefix):
            raise ConfigArgumentError(
                'prefix',
                f'{self._ld_regis["name"]!r} does not starts with the {self.__class__.__name__} '
                f'prefix value {self.load_prefix!r}.'
            )

    def __hash__(self):
        return hash(self.name + self.load_stage)

    def __str__(self):
        return f"({self.name}, {self.load_stage})"

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name})>"

    @property
    def name(self) -> str:
        """Return configuration name without domain name."""
        return self._ld_regis['name']

    @property
    def fullname(self) -> str:
        """Return configuration name which combine with the domain name."""
        return self._ld_regis['fullname']

    @property
    def update_time(self) -> str:
        """Return update timestamp from configuration data."""
        return self._ld_regis['data'].get('update_time')

    @property
    def version(self) -> str:
        """Return version from configuration data."""
        return self._ld_regis['data'].get('version')

    @cached_property
    def type(self) -> ObjectType:
        """Return object type which implement in `config_object` key."""
        if (_typ := self._ld_regis['data'].get('type')) is None:
            logger.warning(f"the 'type' value: {_typ} does not exists in config data.")
            # TODO: add default type from prefix name.
            raise ValueError
        _obj_prefix: str = params.engine.config_object
        return import_string(f"{_obj_prefix}.{_typ}")

    @cached_property
    def _map_data(self) -> dict:
        """Return configuration data without key in the excluded key set."""
        _data: dict = self._ld_regis['data'].copy()
        # print(f"Data before mapping: {_data} \n")
        _results: dict = {k: _data[k] for k in _data if k not in self.excluded_key}

        # Mapping secrets value.
        if self._ld_secrets:
            _results: dict = map_secret(_results)

        # Mapping function result value.
        if self._ld_function:
            _results: dict = map_function(_results)

        # Mapping datetime format to string value.
        for _ in self.datetime_key:
            if hasdot(_, _results):
                # Fill format datetime object to any type value.
                _get: Any = getdot(_, _results)
                _results: dict = setdot(_, _results, map_func_to_str(_get, self.datetime.format))
        return _results

    @property
    def data(self) -> dict:
        """Return deep copy of configuration data."""
        return copy.deepcopy(self._map_data)

    @property
    def parameters(self) -> dict:
        """Return parameters of this loading object"""
        return self._parameters

    @property
    def datetime(self) -> Datetime:
        """Return Datetime formatter object from `audit_date` parameters."""
        return Datetime.parse(
            value=self._parameters[self.load_datetime_name],
            fmt=self.load_datetime_fmt
        )

    @clear_cache(attrs=('type', '_map_data'))
    def refresh(self) -> LoadType:
        """Refresh configuration data. This process will use `deploy` method of the
        register object.
        """
        if self._ld_catalog_flg:
            return BaseLoad.from_catalog(name=self.fullname, refresh=True)
        raise NotImplementedError(
            f'{self.__class__.__name__} with False in catalog flag does not implement refresh method.'
        )

    @clear_cache(attrs=('type', '_map_data'))
    def option(self, key, value) -> LoadType:
        """Set attribute that has a `_` prefix."""
        if key not in self.option_key or not hasattr(self, key):
            raise ConfigArgumentError(
                f'option:{key!r}',
                f'this option method of {self.__class__.__name__!r} object does not support'
            )
        super(BaseLoad, self).__setattr__(f"_{key}", value)
        return self


class Connection(BaseLoad):
    """Connection loading class.
    YAML file structure for connection object,

        <connection-alias-name>:
           (format 01)
           type: <connection-object-type>
           endpoint: `{protocol}://{username}:{password}@{host}:{port}/{database}`

           (format 02)
           type: <connection-object-type>
           host: <host>
           port: <port>
           username: <username>
           password: <password>
           database: <database>

           (optional)
           ssh_tunnel:
               ssh_host: <host>
               ssh_port: <port>
               ssh_user: <username>
               ssh_private_key: <private-key-filepath>

    """

    load_prefix: set = {
        'conn',
        'connection',
    }

    def connect(self):
        """Return the connection instance."""
        return self.type.from_data(self.data)


class Catalog(BaseLoad):
    """Catalog loading class.
    YAML file structure for catalog object,

        <catalog-alias-name>:

            (format 01)
            type: <catalog-object-type>
            connection: <connection-alias-name>
            endpoint: `{schema}.{table}`

            (format 02)
            type: <catalog-object-type>
            connection: <connection-alias-name>
            endpoint: `{sub-path}/{filename}.{file-extension}`

            (optional)
            schemas:
                <column-name>:
                    alias: <source-column-name>::<data-type>,
                    nullable: boolean,
                    pk: boolean,
                    default: <default-value>,
                    unique: boolean
                    ...
                <column-name>: ...
    """

    load_prefix: set = {
        'catl',
        'catalog',
    }

    @property
    def connection(self) -> Connection:
        """Return a connection of catalog"""
        _conn: Union[str, dict] = self.data.get('connection')
        if not _conn:
            raise ConfigArgumentError(
                'connection', 'does not set in Catalog template.'
            )
        elif isinstance(_conn, str):
            return Connection.from_catalog(name=_conn, parameters=self.parameters)
        return Connection.from_dict(
            name=f"conn_form_{self.name}",
            content=_conn,
            parameters=self.parameters
        )

    def load(self, limit: Optional[int] = None, option: Optional[dict] = None) -> pd.DataFrame:
        """Return loading object from the catalog type."""
        with self.connection.connect() as conn:
            return self.type.from_data(self.data).load(conn, limit=limit, option=option)

    def save(self, output, option: Optional[dict] = None) -> None:
        """Saving object to the catalog type from output argument."""
        with self.connection.connect() as conn:
            self.type.from_data(self.data).save(output, conn, option=option)


class Node(BaseLoad):
    """Node loading class.
    YAML file structure for node object,

        <node-alias-name>:

            (format 01)
            type: <node-object-type>
            input:
                - alias: <input-alias-name>
                  from: <input-catalog-alias-name>
                  ...
                - ...
            transform:
                - alias: <transform-output-alias-name>
                  input: [<input-alias-name>, ...]
                  actions:
                      - type: <action-object-type>
                        ...
                      - ...
                - ...
            output:
                - from: <input-alias-name>
                  to: <output-catalog-alias-name>
                  ...
                - ...
    """

    load_prefix: set = {
        'node',
        'trans',
        'transform',
    }

    datetime_key: set = {
        'input',
        'output',
    }

    def catalog(self, name: Union[str, dict]) -> Catalog:
        """Return Catalog object."""
        if isinstance(name, str):
            return Catalog.from_catalog(name, parameters=self.parameters)
        return Catalog.from_dict(
            name=f"catl_form_{self.name}",
            content=name,
            parameters=self.parameters
        )

    @property
    def loading(self) -> Dict[str, dict]:
        """Return loading mapping with Catalog object"""
        return {
            _input.pop('alias'): {
                'data': self.catalog(name=_input.pop('from')),
                'params': _input
            }
            for _input in self.data.get('input', {})
        }

    @property
    def saving(self) -> Dict[str, dict]:
        """Return saving mapping with Catalog object"""
        return {
            _output.pop('from'): {
                'data': self.catalog(name=_output.pop('to')),
                'params': _output
            }
            for _output in self.data.get('output', {})
        }

    def catch(self) -> Any:
        """Return output of node."""
        return self.type.from_data(
            {
                'input': self.loading,
                'transform': self.data.get('transform', [])
            }
        ).runner(catch=True)

    def deploy(self):
        """Deploy node transform to saving catalog."""
        return self.type.from_data(
            {
                'input': self.loading,
                'output': self.saving,
                'transform': self.data.get('transform', [])
            }
        ).runner(catch=False)


class Schedule(BaseLoad):
    """Schedule loading class
    YAML file structure for schedule object,

        <schedule-alias-name>:

            (format 01)
            type: <schedule-object-type>
            cron: <cron>

    """

    load_prefix: set = {
        'schd',
        'schedule',
    }

    @property
    def cronjob(self) -> CronJob:
        """Return the schedule instance."""
        return self.type.from_data(self.data).cron

    def generate(self, start: str) -> CronRunner:
        return self.type.from_data(self.data).schedule(start)


StatusItem: Type = Tuple[int, Optional[str]]


class Status(enum.IntEnum):
    """Status enumerations, which are a set of symbolic names (members)
    bound to unique, constant values

    :usage:
        >>> for sts in Status:
        ...    print(f"Status name: {sts.name!r} have value: {sts.value}")
        Status name: 'READY' have value: -1
        Status name: 'SUCCESS' have value: 0
        Status name: 'FAILED' have value: 1
        Status name: 'PROCESSES' have value: 2

        >>> Status['SUCCESS']
        <Status.SUCCESS: (0, 'Successful')>

        >>> Status(2)
        <Status.PROCESSES: (2, 'Processing')>

        >>> isinstance(Status.SUCCESS, Status)
        True

        >>> Status.SUCCESS is Status.FAILED
        False

        >>> for sts in Status:
        ...     if sts <= Status.FAILED:
        ...         print(sts)
        READY
        SUCCESS
        FAILED
    """
    READY: StatusItem = -1, 'Ready'
    SUCCESS: StatusItem = 0, 'Successful'
    DONE: StatusItem = 0, 'Successful'
    FAILED: StatusItem = 1, 'Error'
    ERROR: StatusItem = 1, 'Error'
    PROCESSES: StatusItem = 2, 'Processing'
    WAITING: StatusItem = 2, 'Processing'

    def __new__(cls, value, *args, **kwargs):
        obj = int.__new__(cls, value)
        obj._value_ = value
        return obj

    def __init__(self, _: int, desc: Optional[str] = None):
        self._description_: str = desc or self.name

    def __repr__(self):
        return f'<{self.__class__.__name__}.{self.name}: ({self.value}, {self.desc!r})>'

    def __str__(self):
        return self.name

    @property
    def desc(self):
        return self._description_


class Pipeline(BaseLoad):
    """Pipeline loading class"""

    load_prefix: set = {
        'pipe',
        'pipeline',
    }

    def schedule(self) -> Schedule:
        ...

    def node(self) -> Node:
        ...

    def generate_report(self, style: str):
        ...

    def tracking(self):
        ...

    def process(self):
        ...


__all__ = [
    'Connection',
    'Catalog',
    'Node',
    'Schedule',
    'Pipeline',
]


def test_conn_search():
    conn_local = Connection('demo:conn_local_file_with_datetime')
    print(conn_local.data.pop('endpoint'))
    conn_local.option(
        'parameters', {'audit_date': '2021-01-01 00:00:00'}
    )
    print(conn_local.data.pop('endpoint'))


def test_node_load():
    node_test = Node('demo:node_seller_prepare')
    node_test.option(
        'parameters', {'audit_date': '2021-01-01 00:00:00'}
    )
    print(node_test.saving)
    results = node_test.catch()
    for k in results:
        print(f'Result {k}: {"-" * 120}')
        print(results[k])
        # print(results[k].dtypes)
    print(node_test.parameters)
    # node_test.deploy()


def test_node_trans():
    node_test = Node('demo:node_seller_transform')
    node_test.option(
        'parameters', {'audit_date': '2021-01-01 00:00:00'}
    )
    print(node_test.loading)
    results = node_test.catch()
    for k in results:
        print(f'Result {k}: {"-" * 120}')
        print(results[k])
        # print(results[k].dtypes)
    # node_test.deploy()


if __name__ == '__main__':
    # test_conn_search()
    # test_node_load()
    test_node_trans()
