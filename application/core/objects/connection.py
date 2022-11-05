# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import re
import os
import logging
import boto3
import fsspec
import functools
from fsspec.spec import AbstractFileSystem
from sshtunnel import SSHTunnelForwarder
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine.url import (
    URL,
    make_url,
)
from sqlalchemy.engine.result import MappingResult
from sqlalchemy.engine.base import (
    Engine,
    Connection,
)
from typing import (
    Optional,
    List,
    Tuple,
    Generator,
    NoReturn,
)
from application.utils.type import merge_dict
from application.io import (
    AssetConf,
    join_path,
)
from application.errors import ConfigArgumentError

logger = logging.getLogger(__name__)

os.environ['APP_PATH'] = os.getenv('APP_PATH', join_path(os.path.abspath(__file__), '../../..'))


class BaseStorageSystem:
    """The Base Storage connection system that use the fsspec library for remote
    creation. This object will design necessary properties and methods for any kind
    of Storage sub class system.
    """

    protocol_names: tuple = tuple()

    protocol_name: str = ""

    path_keys: set = {
        ('protocol', 'drivername', None),
        ('container', 'host', None),
        ('storage', 'database', None),
    }

    path_format: str = "{protocol}://{container}/{storage}"

    @classmethod
    def extract_path_from_dict(cls, data: dict) -> List[dict]:
        """Return the list of dicts which one contain key in `cls.path_keys` and
        other was pop keys from the data.
        """
        _data_merge: dict = merge_dict({'protocol': cls.protocol_name}, data)
        if (_pt := _data_merge['protocol']) != cls.protocol_name:
            raise ConfigArgumentError(
                'protocol', f'the {cls.__name__!r} does not support for protocol {_pt!r}'
            )
        return [
            {
                k[1]: _data_merge.pop(k[0], k[2])
                for k in cls.path_keys
            },
            _data_merge
        ]

    @classmethod
    def from_data(cls, data: dict):
        if not (_path := data.pop('endpoint', None)):
            raise ConfigArgumentError(
                'endpoint', 'the connection data does not contain this necessary key.'
            )
        return cls(path=_path, properties=data)

    def __init__(
            self,
            path: str,
            properties: Optional[dict] = None
    ):
        """Main initialize of the Base Storage system connection object that use the
        path argument for connect to target storage system with standard connection
        string format, like

            {protocol}://{container}/{storage}

        :param path: str : A configuration path of connection.

        :param properties: Optional[dict] : a properties of connection.
        """
        _url: URL = make_url(path)
        if _url.query:
            raise ConfigArgumentError(
                'endpoint',
                'the connection endpoint should not contain any query string in url.'
            )
        self._props: dict = properties or {}
        self._conn_url: URL = _url

    def __str__(self):
        return self.path

    def __repr__(self):
        _props: str = f", properties={self._props}" if self._props else ''
        return f"<{self.__class__.__name__}(path={self._conn_url}{_props})>"

    @property
    def path(self) -> str:
        return self.path_format.format(**self._path_keys)

    @property
    def _path_keys(self):
        """Return mapping of path keys which use for string formatter."""
        return {
            'protocol': self._conn_url.drivername or '',
            'container': self._conn_url.host or '',
            'storage': self._conn_url.database or '',
        }

    @property
    def properties(self) -> dict:
        """Return main properties that set in the same level of any connection keys."""
        return self._props

    @property
    def filesystem(self) -> AbstractFileSystem:
        _protocol: str = (
            _search.group('protocol')
            if (_search := re.search(r'^(?P<protocol>\w+)://', self.path))
            else self.protocol_name
        )
        return fsspec.filesystem(_protocol, **self.properties)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> NoReturn:
        return

    def ls(self, path: Optional[str] = None) -> list:
        return self.filesystem.ls(self.join(self.path, path))

    def walk(self, path: str) -> Generator:
        return self.filesystem.walk(path=self.join(self.path, path))

    def glob(self, path: str) -> list:
        return self.filesystem.glob(path=self.join(self.path, path))

    def exists(self, path: str) -> bool:
        """Return True if path of file or directory exists in target system"""
        return self.filesystem.exists(path=self.join(self.path, path))

    def prepare(self, ):
        """Return prepare filename with parameter."""

    @staticmethod
    def join(*args, sep: str = '/'):
        """Return the joined path by sep string value."""
        return functools.reduce(lambda x, y: sep.join([x, y]) if y else x, args)


class LocalSystem(BaseStorageSystem):
    """The Local File system connection"""

    protocol_name: str = "file"

    path_format: str = "{storage}"


class S3System(BaseStorageSystem):
    """The AWS S3 system connection"""

    protocol_name: str = 's3'

    aws_keys: set = {
        ('aws_access_key_id', None),
        ('aws_secret_access_key', None),
        ('region_name', 'ap-southeast-1'),
        ('role_arn', None),
        ('mfa_serial', None),
    }

    def __init__(
            self,
            path: str,
            properties: Optional[dict] = None
    ):
        """Main initialize of the AWS S3 system connection object."""
        super(S3System, self).__init__(path, properties)
        self._aws_props: dict = {k[0]: self._props.pop(k[0], k[1]) for k in self.aws_keys}
        self._aws_credential: dict = self.sts_credential

    @property
    def properties(self) -> dict:
        return merge_dict({
            'anon': False,
            'key': self._aws_credential['AccessKeyId'],
            'secret': self._aws_credential['SecretAccessKey'],
            'token': self._aws_credential.get('SessionToken', None)
        }, self._props)

    @property
    def filesystem(self) -> AbstractFileSystem:
        return fsspec.filesystem(self.protocol_name, **self.properties)

    @property
    def sts_credential(self) -> dict:
        """Return the AWS credential dictionary from assume role if role_arn was passing
        from the properties.
        """
        sts_client = boto3.client(
            service_name='sts',
            region_name=self._aws_props['region_name'],
            aws_access_key_id=self._aws_props['aws_access_key_id'],
            aws_secret_access_key=self._aws_props['aws_secret_access_key']
        )
        if not (_role := self._aws_props['role_arn']):
            return {
                'AccessKeyId': self._aws_props['aws_access_key_id'],
                'SecretAccessKey': self._aws_props['aws_secret_access_key'],
            }

        _role_name: Optional[str] = self._aws_props.get('role_name', None)
        if _mfa := self._aws_props['mfa_serial']:
            return self.assume_role(sts_client, _role, _role_name, _mfa)
        return self.assume_role(sts_client, _role, _role_name)

    @staticmethod
    def assume_role(
            client,
            role_arn: str,
            role_name: Optional[str] = None,
            mfa_serial: Optional[str] = None,
            optional: Optional[dict] = None
    ):
        """Assume cross account role and return credentials"""
        if mfa_serial:
            mfa_otp: str = str(input("Enter the MFA code: "))
            optional: dict = merge_dict({
                'SerialNumber': mfa_serial,
                'TokenCode': mfa_otp
            }, optional)
        assumed_role_obj = client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=(role_name or 'RoleSessionBoto3Local'),
            DurationSeconds=3600,
            **(optional or {})
        )
        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        return assumed_role_obj['Credentials']


class AzureBlobSystem(BaseStorageSystem):
    """The Azure Blob system connection"""

    protocol_names: tuple = ('abfs', )

    protocol_name: str = 'abfs'

    path_format: str = "{container}/{storage}"

    def ls(self, path: Optional[str] = None) -> list:
        # the `self.ls` method will raise error when the path startswith '{protocol}://'.
        _path = self.join(self.path, path) if path else self.path
        return self.filesystem.ls(_path)

    def glob(self, path: str) -> list:
        # the `self.glob` method will raise error when the path startswith '{protocol}://'.
        return self.filesystem.glob(path=self.join(self.path, path))

    def exists(self, path: str) -> bool:
        # the `self.exists` method will raise error when the path startswith '{protocol}://'.
        return self.filesystem.exists(path=self.join(self.path, path))


class GoogleCloudStorage(BaseStorageSystem):
    """The Google Cloud storage system connection"""

    protocol_name: str = 'gcs'


class FTPSystem(BaseStorageSystem):
    """The FTP system connection"""

    protocol_name: str = 'ftp'


class SFTPSystem(BaseStorageSystem):
    """The SFTP system connection"""

    protocol_name: str = 'sftp'


class BaseRDBMSSystem:
    """The Base RDBMS connection system that use the sqlalchemy for engine creation.
    This object will design necessary properties and methods for any kind of RDBMS
    sub class system.
    """

    driver_name: str = ""

    default_schema: str = ""

    asset_version: str = ""

    url_keys: set = {
        ('drivername', None),
        ('username', None),
        ('password', None),
        ('host', None),
        ('port', None),
        ('database', None),
        ('query', None),
    }

    @classmethod
    def extract_url_from_dict(cls, data: dict) -> List[dict]:
        """Return the list of dicts which one contain the key in `cls.url_keys` and other pop
        that the keys. The input data will already merge by {'drivername': `cls.driver_name`}
        value so this class method will validate a drivername key in it's key.
        """
        _data_merge: dict = merge_dict({'drivername': cls.driver_name}, data)
        if (_dn := _data_merge['drivername']) != cls.driver_name:
            raise ConfigArgumentError(
                'drivername', f'the {cls.__name__!r} does not support for drivername {_dn!r}'
            )
        return [
            {
                k[0]: _data_merge.pop(k[0], k[1])
                for k in cls.url_keys
            },
            _data_merge
        ]

    @classmethod
    def from_data(cls, data: dict):
        """Return the Base RDBMS system connection object that use input argument from
        data dictionary.
        """
        if _url := data.pop('endpoint', None):
            return cls(url=_url, properties=data)

        url_mapping, data = cls.extract_url_from_dict(data)
        if url_mapping:
            return cls(str(URL.create(**url_mapping)), properties=data)
        raise ConfigArgumentError(
            'endpoint', f'the {cls.__name__!r} data does not contain this necessary key.'
        )

    def __init__(
            self,
            url: str,
            properties: Optional[dict] = None
    ):
        """Main initialize of the Base RDBMS system connection object that use the url
        argument for connect to target RDBMS system with standard connection string
        format, like

            {protocol}://{username}:{password}@{host}:{port}/{database}

            In this initialization, the input url should not contain any query string
        because the process will use query from input properties only.

        :param url: str : A configuration url of database connection

        :param properties: Optional[dict] : A properties of connection
        """
        _url: URL = make_url(url)
        if _url.query and all(_ not in _url.drivername for _ in {'mssql', 'pyodbc', }):
            raise ConfigArgumentError(
                'endpoint',
                'the connection endpoint should not contain any query string in url.'
            )
        self._props: dict = properties or {}
        self._ssh_tunnel: Optional[dict] = self._props.pop('ssh_tunnel', None)
        self._conn_url: URL = _url
        self._conn_connect: Optional[Connection] = None
        self._conn_server: Optional[SSHTunnelForwarder] = None

    def __str__(self):
        return self.url

    def __repr__(self):
        _props: str = f", properties={self._props}" if self._props else ''
        return f"<{self.__class__.__name__}(url={self._conn_url}{_props})>"

    @property
    def url(self) -> str:
        """Return a url string."""
        return str(self._conn_url)

    @property
    def is_private(self) -> bool:
        return bool(self._ssh_tunnel)

    @property
    def assets(self):
        """Return a assets with match with `self.driver_name` key."""
        # TODO: filter assets version with `self.asset_version`
        assets = AssetConf('application/core/object/assets')
        return getattr(assets, self.driver_name)

    @property
    def properties(self) -> dict:
        """Return main properties that set in the same level of any connection keys."""
        return merge_dict({
            'encoding': 'utf-8',
            'echo': False,
            'pool_pre_ping': True,
        }, self._props)

    @property
    def engine(self) -> Engine:
        if self.is_private:
            self._conn_server = SSHTunnelForwarder(**{
                'ssh_address_or_host': (self._ssh_tunnel['ssh_host'], int(self._ssh_tunnel.get('ssh_port', 22))),
                'ssh_username': self._ssh_tunnel['ssh_user'],
                'ssh_private_key': self._ssh_tunnel['ssh_private_key'],
                'remote_bind_address': (self._conn_url.host, int(self._conn_url.port)),
                'local_bind_address': ('localhost', int(self._conn_url.port)),
            })
            if not self._conn_server.is_alive:
                self._conn_server.start()
        return sqlalchemy.create_engine(self.bind_url(self._conn_url), **self.properties)

    @property
    def engine_auto_commit(self) -> Engine:
        return self.engine.execution_options(isolation_level="AUTOCOMMIT")

    @property
    def connection(self) -> Optional[Connection]:
        return self._conn_connect

    def close(self) -> NoReturn:
        """Close connection variable and change value to None."""
        if self._conn_connect:
            self._conn_connect.close()
            self._conn_connect: Optional[Connection] = None
        if self._conn_server:
            self._conn_server.stop()
            self._conn_server: Optional[SSHTunnelForwarder] = None

    def __enter__(self):
        if not self._conn_connect:
            self._conn_connect: Connection = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> NoReturn:
        self.close()

    def bind_url(self, url: URL) -> URL:
        """Return URL with binding host if private connection via SSH Tunnel."""
        return URL.create(
            drivername=url.drivername,
            host='localhost',
            username=url.username,
            password=url.password,
            database=url.database,
            port=url.port,
        ) if self.is_private else url

    def execute(self, *args, **kwargs):
        return self._conn_connect.execute(*args, **kwargs)

    def rowcount(self, *args, **kwargs) -> int:
        return self.execute(*args, **kwargs).rowcount

    def select(self, *args, **kwargs) -> MappingResult:
        return self.execute(*args, **kwargs).mappings()

    def columns(self, table: str) -> list:
        """Return the list of all column properties of a table in the RDBMS
        subclass system.
        """
        raise NotImplementedError

    def tables(self) -> list:
        """Return the list of all table in the RDBMS subclass system."""
        raise NotImplementedError

    def table_exists(self, table: str) -> bool:
        """Return True if a input table exists in the RDMS subclass system."""
        raise NotImplementedError

    def get_schema(self, table: str, schema: Optional[str] = None) -> Tuple[str, str]:
        """Return pair of table and schema."""
        if '.' not in table:
            _schema: str = schema or self.default_schema
        else:
            _schema, table = table.rsplit('.', maxsplit=1)
        return _schema, table


class SQLiteSystem(BaseRDBMSSystem):
    """The SQLite system connection object."""

    driver_name: str = 'sqlite'

    def columns(self, table: str) -> list:
        with self as conn:
            rows = conn.select(text(self.assets.base.show.columns.format(table=table)))
            result: list = rows.all()
        return result

    def tables(self) -> list:
        """Return the list of all table in the SQLite database."""
        with self as conn:
            rows = conn.select(text(self.assets.base.show.tables))
            result: list = rows.all()
        return result

    def table_exists(self, table: str) -> bool:
        """Return True if a input table exists in the SQLite database."""
        with self as conn:
            rows = conn.execute(text(self.assets.base.exists.table.format(table=table)))
            result = rows.fetchone()
        return bool(result[0])

    def get_schema(self, table: str, schema: Optional[str] = None) -> Tuple[str, str]:
        raise NotImplementedError


class PostgresSystem(BaseRDBMSSystem):
    """The PostgreSQL system connection"""

    driver_name: str = 'postgresql'

    default_schema: str = 'public'

    def columns(self, table: str, schema: Optional[str] = None) -> list:
        with self as conn:
            rows = conn.select(text(self.assets.base.show.columns.format(table=table)))
            result: list = rows.all()
        return result

    def tables(self, schema: Optional[str] = None) -> list:
        _schema: str = schema or 'public'
        with self as conn:
            rows = conn.select(text(self.assets.base.show.tables.format(schema=_schema)))
            result: list = rows.all()
        return result

    def table_exists(self, table: str, schema: Optional[str] = None) -> bool:
        """"""
        schema, table = self.get_schema(table, schema)
        with self as conn:
            rows = conn.execute(text(self.assets.base.exists.table.format(table=table, schema=schema)))
            result = rows.fetchone()
        return bool(result[0])

    def schema_exists(self, schema: str):
        """"""
        with self as conn:
            rows = conn.execute(text(self.assets.base.exists.schema.format(schema=schema)))
            result = rows.fetchone()
        return bool(result[0])


class SQLServerSystem(BaseRDBMSSystem):
    """The Microsoft SQL Server system connection"""

    driver_name: str = 'mssql'

    default_schema: str = 'adb'

    def columns(self, table: str) -> list:
        ...

    def tables(self):
        ...

    def table_exists(self, table: str) -> bool:
        ...


class MySQLSystem(BaseRDBMSSystem):
    """The MySQL system connection"""

    def columns(self, table: str) -> list:
        ...

    def tables(self):
        ...

    def table_exists(self, table: str) -> bool:
        ...


class GBQSystem:
    ...


class RedShiftSystem:
    ...


class AzureSynapseSystem:
    ...


class MangoDBSystem:
    """"""


class RedisSystem:
    """"""


__all__ = [
    # File System objects
    'LocalSystem',
    'S3System',
    'AzureBlobSystem',

    # RDBMS System objects
    'SQLiteSystem',
    'PostgresSystem',
]


def test_url():
    url: str = 'protocol://username:password@host:1234/database?key1=value1&key1=value2'
    a: URL = sqlalchemy.engine.make_url(url)
    print(bool(a.query))
    url: str = 'protocol://username:password@host:1234/database'
    a: URL = sqlalchemy.engine.make_url(url)
    print(bool(a.query))


def test_local():
    path: str = 'file:///D:/korawica/Work/dev02_miniproj/GITHUB/dde-object-defined/data'
    # with LocalSystem(path=path) as conn:
    #     print(conn.filesystem)
    #     print(isinstance(conn.filesystem, AbstractFileSystem))
    #     print(conn.ls())
    a = make_url(path)
    print(a.drivername)
    print(a.host)
    print(a.database)


def test_s3():
    path: str = 's3://{bucket-name}/{object-name}'
    a = make_url(path)
    print(a.drivername)
    print(a.host)
    print(a.database)


def test_adls():
    ...


def test_gcs():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.'
    from gcsfs import GCSFileSystem
    gcs = GCSFileSystem(project='', token=None)


def test_sqlite():
    root: str = 'D:/korawica/Work/dev02_miniproj/GITHUB/dde-object-defined/data'
    with SQLiteSystem(f'sqlite:///{root}/metadata.db') as conn:
        # rows = conn.execute(text("select * from pragma_table_info('tbl_metadata') as pragma"))
        rows = conn.execute(text("select * from sqlite_master where name='tbl_metadata'"))
        result = rows.mappings()
        print(type(result))
        for _ in result.all():
            print(_)
        print(conn.columns('tbl_metadata'))


def test_mssql():
    odbc_string = (
        r'Driver={ODBC Driver 17 for SQL Server};'
        r'Server=tcp:di-sql-dev.database.windows.net,1433;'
        r'Database=DWHCTRLDEV;'
        r'Uid=korawica@scg.com;'
        # r'Pwd=kora#Oct22;'
        r'Encrypt=yes;'
        r'TrustServerCertificate=yes;'
        r'Connection Timeout=30;'
        # r'Authentication=ActiveDirectoryIntegrated;'
        r'Authentication=ActiveDirectoryInteractive;'
    )
    with SQLServerSystem(f'mssql:///?odbc_connect={odbc_string}') as conn:
        print(conn)


if __name__ == '__main__':
    test_local()
    test_s3()
    # test_adls()
    # test_gcs()
    # test_sqlite()
    # test_mssql()
    # test_url()
    # print(URL.create(
    #     drivername='postgres+psycopg',
    #     host='localhost',
    # ))
