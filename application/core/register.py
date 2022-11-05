# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import re
import logging
import packaging.version
from deepdiff import DeepDiff
from datetime import datetime
from functools import (
    cache,
    cached_property,
    total_ordering
)
from typing import (
    Optional,
    Any,
    Tuple,
    List,
    TypeVar,
    Callable
)
from .config import (
    ConfFile,
    ConfMetadata,
    ConfLogging,
    ConfLoader,
    env,
    params
)
from .formatter import (
    Datetime,
    Version,
    OrderFormat
)
from ..io import (
    join_path,
    join_root_with,
)
from ..utils.type import (
    concat,
    merge_dict,
    hash_dict,
    freeze_args,
)
from ..utils.reusables import (
    get_date, clear_cache
)
from ..errors import (
    ConfigArgumentError,
    ConfigNotFound,
)


logger = logging.getLogger(__name__)


class Environment:
    """Environment object which validate environment value for registration process.
    the environment value from local environment variable will change tos shortname,
    such as from `development` to `dev`. When register object was initialized, the
    configuration data will loading from the environment path that combine between
    domain (if domain was set) and environment shortname.
    """

    __slots__ = '_env'

    env_app_name: str = 'APP_ENV'

    env_rename: tuple = (
        'sandbox',
        'dev',
        'test',
        'sit',
        'uat',
        'prod',
    )

    def __init__(self, name: Optional[str] = None):
        self._env: Optional[str] = None
        if params.engine.config_environment:
            if not (_env := env.get(self.env_app_name, None)):
                # Raise if engine setting file, `parameter.yaml` set be Ture but `APP_ENV`
                # does not set in environment variable.
                raise ConfigArgumentError(
                    'environment',
                    f'the key -> `config_environment` was not set in engine setting file '
                    f'or `{self.env_app_name}` was not set in environment variable.'
                )
            # Convert environment value to initials
            _env_edit: str = _env.strip("'").strip('"').lower()
            self._env: str = next(
                (_ for _ in self.env_rename if _env_edit.startswith(_)),
                _env_edit
            )
        elif name:
            assert name in self.env_rename, f'a `name` argument must exists in ' \
                                            f'{self.env_rename} only.'
            # Force define environment value from input argument.
            self._env: str = name

    @property
    def name(self) -> str:
        """Return a name of environment and return empty string if environment does
        not set.
        """
        return self._env or ''

    @property
    def path(self) -> str:
        """Return a path of environment and return empty string if environment does
        not set.
        """
        return f"{self._env}/" if self._env else ''

    @property
    def exists(self) -> bool:
        """Return True if environment was set."""
        return bool(self._env)


class BaseRegister:
    """Base Register Object which implement necessary base properties and methods for
    registation with any configuration name and domain (the domain is optional depend
    on `config_domain` parameter). The input argument will support only lower case
    string which mean we will cast to lower case in initialization process.

        The configuration name should be the unique key in files or the first column
    in table. If it need to be contain duplicate name, it should define the version
    with datetime format for sorting the latest configuration data from that name like,

    :example:

        - Case the configuration data keep by file,

            (i)     <config-name>:
                        version: '{year}-{month}-01'
                        ...

            (ii)    <config-name>:
                        version: '{year}-{month}-02'
                        ...

        - Case the configuration data keep by table,

            config_name     | update_date           | ...
            ----------------+-----------------------+----
            <config-name>   | '{year}-{month}-01'   | ...
            <config-name>   | '{year}-{month}-02'   | ...

        The searching engine will get the second day from duplication key of config data.
    """

    domain_ptt: str = ':'

    name_ptt: str = '_'

    def __init__(
            self,
            name: str,
            domain: Optional[str] = None,
    ):
        """Main instance initialization of configuration that use name, which should
        not contain any string partition like `,` or `:`, and domain, which replace
        OS separation to `/` such as

            (i)     '<main-domain>\\<sub-domain>' to '<main-domain>/<sub-domain>'

            (ii)    '<main-domain>/<sub-domain>/' to '<main-domain>/<sub-domain>'

            Additional, this process will include configuration partitioning with
        different environment if `config_environment` set be true. The loading process
        pick the configuration data from union of path between environment and domain
        together.

        :param name: str : The name of configuration that is the first key of key-value
                document or first column of column-base document. This value should not
                contain comma (`,`) or dot (`,`) because the name should partition with
                underscore (`_`) only.

        :param domain: Optional[str] : The domain name is the name of directory that
                contain any files of the config name.
        """
        self._cf_name: str = name.lower()
        self._cf_domain: Optional[str] = None
        self._cf_env: Environment = Environment()

        # Set domain variable if the `domain` argument was passed
        if domain:
            self._cf_domain: Optional[str] = (
                domain.replace(os.sep, '/').rstrip('/').lower()
            )

        # Validate input arguments and raise error step.
        if any(sep in self._cf_name for sep in {',', '.'}):
            # Raise if name of configuration contain comma (`,`) or dot (`.`) characters.
            raise ConfigArgumentError(
                'name', 'the name of configuration should not contain comma character'
            )
        elif not params.engine.config_domain and self._cf_domain:
            # Raise if engine setting file, `parameter.yaml` does not set `config_domain`.
            raise ConfigArgumentError(
                'domain', 'the key -> `config_domain` was not set in engine setting file.'
            )

    def __hash__(self):
        return hash(self.fullname)

    def __str__(self):
        return self.fullname

    def __repr__(self):
        _params: list = [f"name={self.name!r}"]
        if self.domain:
            _params.append(f"domain={self.domain!r}")
        return f"<{self.__class__.__name__}({', '.join(_params)})>"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return (
                    self.name == other.name
                    and self.domain == other.domain
            )
        return False

    @cached_property
    def name(self) -> str:
        """Return a configuration name."""
        return self._cf_name

    @property
    def fullname(self) -> str:
        """Return a configuration fullname, which join `name` and `domain` together with
        domain partition string.
        """
        if self.domain:
            return f'{self.domain}{self.domain_ptt}{self._cf_name}'
        return self._cf_name

    @property
    def shortname(self) -> str:
        """Return a configuration shortname, which get first character of any split string
        by name partition string.
        """
        return concat(map(lambda word: word[0], self._cf_name.split(self.name_ptt)))

    @property
    def name_camel(self) -> str:
        """Return a configuration name with camel case with lower case first letter.
        """
        return self.name[0].lower() + self.name_camel_upper[1:]

    @property
    def name_camel_upper(self) -> str:
        """Return a configuration name with camel case that reference by `inflection`.
        """
        return re.sub(r'(?:^|_)(.)', lambda m: m.group(1).upper(), self.name)

    @property
    def domain(self) -> str:
        """Return a domain name. If the value does not set, this property will
        return empty string.
        """
        return self._cf_domain or ''

    @property
    def environ(self) -> str:
        """Return a environment name. If the value does not set, this property will
        return empty string.
        """
        return self._cf_env.name

    @property
    def domain_path(self) -> str:
        """Return a domain path for path walking."""
        return f"{self._cf_env.path}{self.domain}"

    @freeze_args
    @cache
    def formatter(self, mapping: Optional[dict] = None) -> dict:
        """Return the Generated formatter of constraint-string value like name, domain,
        or environ. Support mapping formatter,

            - name: the configuration name
                %n  : normal name
                %N  : normal name with upper case
                %s  : shortname
                %S  : shortname with upper case
                %f  : fullname
                %F  : fullname with upper case
                %c  : camel case
                %-c  : camel case with upper case of first character
            - domain: the domain name
                %n  : normal name
                %N  : normal name with upper case
                %u  : normal name removed vowel
                %U  : normal name removed vowel with upper case
            - environ: the environment name
                %n  : normal name
                %N  : normal with upper case
                %u  : normal name removed vowel
                %U  : normal name removed vowel with upper case

        :param mapping: Optional[dict] : Default None, a additional mapping which want to
                update or replace existing mapping.
        """
        self_fullname: str = self.fullname.replace(self.domain_ptt, '_')
        self_domain_non_vowel: str = re.sub(r'[aeiouAEIOU]', '', self.domain)
        self_environ_non_vowel: str = re.sub(r'[aeiouAEIOU]', '', self.environ)
        _mapping: dict = merge_dict({
            'name': {
                '%n': {'value': self.name},
                '%N': {'value': self.name.upper()},
                '%s': {'value': self.shortname},
                '%S': {'value': self.shortname.upper()},
                '%f': {'value': self_fullname},
                '%F': {'value': self_fullname.upper()},
                '%c': {'value': self.name_camel},
                '%-c': {'value': self.name_camel_upper},
            },
            'domain': {
                '%n': {'value': self.domain},
                '%N': {'value': self.domain.upper()},
                '%u': {'value': self_domain_non_vowel},
                '%U': {'value': self_domain_non_vowel.upper()},
            },
            'environ': {
                '%n': {'value': self.environ},
                '%N': {'value': self.environ.upper()},
                '%u': {'value': self_environ_non_vowel},
                '%U': {'value': self_environ_non_vowel.upper()},
            },
        }, (mapping or {}))
        # Add `regex` with the same data as value because it is constraint for regular
        # expression.
        for fmt in _mapping:
            _get: dict = _mapping[fmt].copy()
            for fmt_str, values in _get.items():
                _mapping[fmt][fmt_str]['regex']: str = values['value']
        return _mapping


@total_ordering
class Stage:
    """Stage Object for validate stage property data from parameter.yaml file and comparable
    to other stage object.
    """

    final: str = list(params.stages.keys())[-1]

    format_keys: set = {
        'name',
        'domain',
        'environ',
        'timestamp',
        'version',
        'compress',
        'extension',
    }

    rules_exists: set = {
        'timestamp',
        'version',
        'compress'
    }

    __slots__ = (
        '_st_name',
        '_st_index',
        '_st_data',
        '__dict__',
    )

    def __init__(self, name: str):
        """Main initialize of the stage object. This process will validate the stage name
        that should exists in parameter.yaml file and the stage properties,

            - format: format string value must contain any formatter name.

            - format: a existing name of formatter must exists in `cls.format_keys`

            - rules: if set a formatter rules, the formatter name should exists format.

        :param name: str : A name of stage which it should exists in parameter.yaml file
        """
        self._st_name: str = name.strip().strip('"').strip("'")
        self._st_index: int = -1
        self._st_data = self.pull()
        # Check a format string of stage exists.
        if not (_fmt := self._st_data.get('format')):
            raise ConfigArgumentError(
                'format', f'in stage {self.name!r} does not set format string.'
            )

        # Validate the name in format string should contain any format name.
        if not (_searches := re.finditer(r'{(?P<name>\w+):?(?P<format>[^{}]+)?}', _fmt)):
            raise ConfigArgumentError(
                'format', f'in stage {self.name!r} dose not include any format name, '
                          f'the stage file was duplicated.'
            )

        # Validate the name in format string should exists in `cls.format_keys`.
        if any((_search.group('name') not in self.format_keys) for _search in _searches):
            raise ConfigArgumentError(
                'format', f'in stage {self.name!r} have an unsupported format name.'
            )

        # Validate a format of stage that relate with rules.
        for validator in self.rules_exists:
            if self._st_data.get('rules', {}).get(validator) and (validator not in _fmt):
                raise ConfigArgumentError(
                    ('format', validator, ),
                    f'the stage {self.name!r} set {validator} rule but does not set '
                    f'{validator} format.'
                )

    def __str__(self):
        return self._st_name

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self._st_name})>"

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.name == other.name
            and self.index == other.index
        )

    def __lt__(self, other) -> bool:
        return self.index < other.index

    @property
    def name(self) -> str:
        """Return a stage name."""
        return self._st_name

    @cached_property
    def format(self) -> str:
        """Return a format of stage."""
        return self._st_data['format']

    @cached_property
    def rules(self) -> dict:
        """Return a rule of stage."""
        return self._st_data.get('rules', {})

    @property
    def index(self) -> int:
        """Return a index of this stage base on all stage in `parameter.yaml` file."""
        return self._st_index

    @property
    def is_final(self) -> bool:
        """Return True if the stage instance is the final stage base on all stage in
        `parameter.yaml` file.
        """
        return (
                self.index == len(params.stages)
                and self.name == self.final
        )

    @clear_cache(attrs=('rules', 'format'))
    def refresh(self):
        """Fetch data from `parameter.yaml` file."""
        self._st_data: dict = self.pull()
        return self

    def pull(self) -> dict:
        """Return the stage properties data from `parameter.yaml` file. The structure
        of stage in parameter file should be,

            stages:
                <stage-name>:
                    format: <format-of-file>
                    rules:
                        timestamp: <retention-timestamp-value>
                        timestamp_metric: <retention-timestamp-metric-value>
                        version: <retention-version-value>
                        compress: <compress-type-of-file>
                        exclude: [<key-to-exclude-01>, ...]

            The value in format string will validate from initialize process that check
        the both of formatter name and it's rules should align with the same value.
        """
        if self.name not in params.stages:
            raise ConfigArgumentError(
                'stage',
                f'Cannot get stage: {self.name!r} because it does not set in `parameter.yaml`'
            )
        self._st_index: int = list(params.stages.keys()).index(self.name)
        return params.stages[self.name].copy()


Registry = TypeVar('Registry', bound='Register')


class Register(BaseRegister):
    """Register Object that contain configuration loading methods and metadata management.
    This object work with stage input argument, that set all properties in the `parameter.yaml`
    file.
    """

    exclude_keys: set = {'version', 'update_time', }

    file_extension: str = 'json'

    datetime_fmt: str = '%Y-%m-%d %H:%M:%S'

    @classmethod
    def reset(
            cls,
            name: str,
            *,
            author: Optional[str] = None,
    ) -> Registry:
        """Reset all of configuration data file that exists in any stage but does not do
        anything in base.

        :param name: str : The fullname of configuration.

        :param author: Optional[str] : ...
        """
        _name: str = concat(name.split())
        if cls.domain_ptt in _name:
            _, _name = _name.rsplit(cls.domain_ptt, maxsplit=1)
        # Delete all config file from any stage.
        for stage in params.stages:
            cls(
                name=name,
                stage=stage,
                author=author,
                auto_update=False,
                force_exists=True,
            ).remove()
        # Delete data form metadata.
        ConfMetadata(
            params.engine.config_metadata, name=_name, environ=Environment().name
        ).remove()
        return cls(name, author=author)

    def __init__(
            self,
            name: str,
            *,
            stage: Optional[str] = None,
            author: Optional[str] = None,
            auto_update: bool = True,
            force_exists: bool = False,
    ):
        """Main instance initialization of register object that use fullname for manage
        configuration data

        :param name: str : The fullname of configuration that was combined by name and
            domain together like,

                (i)     `<domain-name>:<config-name>` with ':' domain partition

                (ii)    `<config-name>` if does not have domain name

        :param stage: Optional[str] : A stage name from parameter.yaml file that want
                to get the latest configuration.

        :param author: Optional[str] : ...

        :param auto_update: bool : ...

        :param force_exists: bool : A flag for force data exists if config data was empty.
        """
        # Initial `_domain` variable for default if name does not contain domain partition
        _name: str = concat(name.split())
        _domain: Optional[str] = None
        if self.domain_ptt in _name:
            _domain, _name = _name.rsplit(self.domain_ptt, maxsplit=1)

        # Initial from parent class
        super(Register, self).__init__(name=_name, domain=_domain)

        # Load latest version of data from data lake or data store of configuration files
        self._cf_auto_update: bool = auto_update
        self._cf_archive: bool = False
        self._cf_stage: Optional[str] = stage
        self._cf_author: Optional[str] = author
        self._cf_data: dict = self.pick(stage=stage)
        if not self._cf_data and not force_exists:
            _domain_stm: str = f'with domain {self.domain!r}' if self.domain else ''
            raise ConfigNotFound(
                f"Configuration {self.name!r} {_domain_stm} "
                f"does not found in the {self.stage!r} data lake or data store."
            )

        # Generate data for all conditions of the Register object
        self.update_time: datetime = get_date('datetime')
        self._meta: ConfLoader = ConfMetadata(
            params.engine.config_metadata, name=self.name, environ=self.environ
        )
        self._log: ConfLoader = ConfLogging(
            params.engine.config_logging, name=self.name, _logger=logger, environ=self.environ
        )

        # Compare data from current stage and latest version in metadata.
        self._cf_data_hash: dict = hash_dict(self.data, exclude_keys=self.exclude_keys)
        self._cf_diff_level: int = self.compare_data(
            target=self.metadata['conf_data'].get(self.stage, {})
        )

        if params.engine.config_stage_archive:
            self._log.p_debug('Archive process was set, so all conf file will move to archive '
                              'before retention.')
            self._cf_archive: bool = True

        # Update metadata if the configuration data does not exists or it has any changes.
        if not self._cf_auto_update:
            self._log.p_debug("Skip update metadata table/file ...")
        elif self._cf_diff_level == 99:
            self._log.p_debug(f"Configuration data with stage: {self.stage!r} does not exists "
                              f"in metadata ...")
            self._log.p_debug("The Process will automate update this data to metadata ...")
            self.update_meta(config_data={
                self.stage: merge_dict({
                    'update_time': f"{self.update_time:{self.datetime_fmt}}",
                    'version': f"v{str(self.version())}"}, self._cf_data_hash
                )})
        elif self._cf_diff_level > 0:
            self._log.p_debug(f"Should update metadata because difference level is {self._cf_diff_level}")
            _version_stm: str = f"v{str(self.version(True) if self._cf_stage else self.version())}"
            self.update_meta(config_data={
                self.stage: merge_dict(
                    self._cf_data_hash, {
                        'update_time': f"{self.timestamp:{self.datetime_fmt}}",
                        'version': _version_stm
                    })})

        # Save logging.
        self._log.save_logging()

    def __hash__(self):
        return hash(self.fullname + self.stage)

    def __str__(self):
        return f"({self.fullname}, {self.stage}, {self.author})"

    def __repr__(self):
        _params: list = [f"name={self.fullname!r}"]
        if self.stage != 'base':
            _params.append(f"stage={self.stage!r}")
        if self.author != 'unknown':
            _params.append(f"author={self.author!r}")
        return f"<{self.__class__.__name__}({', '.join(_params)})>"

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return (
                    self.name == other.name
                    and self.domain == other.domain
                    and self.stage == other.stage
                    and self.author == other.author
            )
        return False

    @property
    def stage(self) -> str:
        """Return the stage name. If the stage name does not pass from init process,
        this property will return default `base` string value.
        """
        return self._cf_stage or 'base'

    @property
    def author(self) -> str:
        """Return the author name. If the author name does not pass from init process,
        this property will return default `unknown` string value.
        """
        return self._cf_author or 'unknown'

    @author.setter
    def author(self, name: str) -> None:
        """Setter of author name."""
        self._cf_author: str = name

    @property
    def data(self) -> dict:
        """Return the data with the configuration name"""
        if self.stage == 'base':
            return merge_dict({
                k: v for k, v in self.metadata['conf_data'].get(self.stage, {}).items()
                if k in {'update_time', 'version', }
            }, self._cf_data)
        return self._cf_data

    @property
    def data_hash(self) -> dict:
        """Return the hashed config data"""
        return self._cf_data_hash

    @property
    def changed(self) -> int:
        """Return the changed level of config data such as 1,2, or 3."""
        return self._cf_diff_level

    @property
    def timestamp(self) -> datetime:
        """Return the current timestamp value of config data. If timestamp value does not
        exists. this property will return timestamp of initialize.

        :return: datetime
        """
        if self.changed > 0:
            return self.update_time
        elif _dt := self.data.get('update_time'):
            return datetime.strptime(_dt, self.datetime_fmt)
        return self.update_time

    def version(self, _next: bool = False) -> packaging.version.Version:
        """Generate version value from the pick method. If version value does not
        exists from configuration data, this property will return the default, `v0.0.1`.
        If the initialization process tracking some change from configuration data
        between metadata and the latest data in the stage, the _next will be generated.

        :return: packaging.version.Version
        """
        _vs = packaging.version.parse(self.data.get('version', 'v0.0.1'))
        if not _next or _vs == 0:
            return packaging.version.parse(self.data.get('version', 'v0.0.1'))
        elif self.changed >= 3:
            return packaging.version.parse(f"v{str(_vs.major + 1)}.0.0")
        elif self.changed == 2:
            return packaging.version.parse(f"v{_vs.major}.{str(_vs.minor + 1)}.0")
        return packaging.version.parse(f"v{_vs.major}.{_vs.minor}.{str(_vs.micro + 1)}")

    @property
    def metadata(self) -> dict:
        """The metadata data filter by the configuration name and merge to
        default values if the data does not contain any needed values.

        :matrices:
            conf_name       : Configuration name
            conf_shortname  : Configuration shortname
            conf_fullname   : Configuration fullname
            conf_data       : Configuration data was hashed and mapped by a stage
            update_time     : Update datetime which start at called registration time
            register_time   : Register datetime
            author          : Configuration author name

        """
        _metadata: dict = self._meta.load()
        if not _metadata:
            # FIXME: fix bug for get empty data from ConfMeta.
            self._log.p_debug(
                f"Metadata does not exists for {self.fullname!r} in stage: {self.stage!r}, "
                f"so the process will return default data.",
                force=True
            )
        _data: dict = _metadata.pop('conf_data', {})
        return merge_dict(
            {
                'conf_name': self.name,
                'conf_shortname': self.shortname,
                'conf_fullname': self.fullname,
                'conf_data': _data,
                'update_time': f"{self.update_time:{self.datetime_fmt}}",
                'register_time': f"{self.update_time:{self.datetime_fmt}}",
                'author': self.author
            }, _metadata
        )

    def update_meta(self, data: Optional[dict] = None, config_data: Optional[dict] = None) -> None:
        """Update data to metadata table or file with optional new data and new
        config data.

        :param data: Optional[dict]

        :param config_data: Optional[dict]
        """
        data: dict = merge_dict(
            {'update_time': f"{self.update_time:{self.datetime_fmt}}", 'author': self.author},
            (data or {})
        )
        if config_data:
            data: dict = merge_dict(
                data, {'conf_data': merge_dict(self.metadata['conf_data'], config_data), }
            )
        return self._meta.save(data=merge_dict(self.metadata, data))

    def compare_data(self, target: dict) -> int:
        """Return difference column from dictionary comparison method which use
        the `deepdiff` library.

        :param target: dict : The target dictionary for compare with current
                configuration data.
        """
        if not target:
            return 99

        results = DeepDiff(
            self._cf_data_hash, target, ignore_order=True,
            exclude_paths={f"root[{key!r}]" for key in self.exclude_keys}
        )

        if any(_ in results for _ in {
            'dictionary_item_added', 'dictionary_item_removed', 'iterable_item_added',
            'iterable_item_removed',
        }):
            return 2
        elif any(_ in results for _ in {'values_changed', 'type_changes', }):
            return 1
        return 0

    def stage_path(self, stage: str) -> str:
        """Return the generated path of stage that combine between environment value and
        stage name if environment mode was set.
        """
        _env: str = f"{self.environ}/" if self.environ else ''
        return f"{_env}{stage}"

    @staticmethod
    def stage_rules(stage: str) -> dict:
        """Return the rules value from stage object and get default value for `base`."""
        return {} if stage == 'base' else Stage(name=stage).rules

    def stage_files(self, stage: str) -> dict:
        """Return the list of files that was mapped with the `OrderFormat` object.

        :param stage: str
        """
        loading = ConfFile(
            path=join_root_with(join_path(env.ARCHIVE_PATH, self.stage_path(stage)))
        )
        results: dict = {}
        for index, file in enumerate(
                map(lambda _f: _f.rsplit('/', maxsplit=1)[-1], loading.files()),
                start=1
        ):
            try:
                results[index]: dict = {
                    'parse': OrderFormat(self.parser(stage, file)),
                    'file': file
                }
            except ConfigArgumentError:
                continue
        return results

    def pick(
            self,
            stage: Optional[str] = None,
            *,
            order: Optional[int] = 1,
            reverse: bool = False
    ) -> dict:
        """Pick latest configuration data from data lake or data store which config in
        `parameter.yaml` or setting file or pick the latest configuration data from a stage
        zone which pass in `stage` argument.

        :param stage: Optional[str]

        :param order: Optional[int] : Default 1, that is the first element from sorted results

        :param reverse: bool : Default False, the reverse flag for files sorting. If the value
                was set to True, this method will return the oldest version configuration data
                from stage.
        """
        # Load data from source
        if not stage or (stage == 'base'):
            return ConfFile(
                path=join_root_with(join_path(env.CONF_PATH, self.domain_path))
            ).load_base(name=self.name, order=order)

        loading = ConfFile(
            path=join_root_with(join_path(env.ARCHIVE_PATH, self.stage_path(stage))),
            compress=self.stage_rules(stage).get('compress'),
            _type=self.file_extension
        )

        if results := self.stage_files(stage):
            max_data: List = sorted(results.items(), key=lambda x: (x[1]['parse'], ), reverse=reverse)
            return loading.load_stage(name=max_data[-order][1]['file'])
        return {}

    def switch(self, stage: str) -> Registry:
        """Switch instance from old stage to new stage with input argument."""
        return self.__class__(
            name=self.fullname,
            stage=stage,
            author=self._cf_author,
            auto_update=self._cf_auto_update,
        )

    def move(
            self,
            stage: str,
            *,
            force: bool = False,
            retention: bool = True
    ) -> Registry:
        """Move configuration to landing stage which set in `parameter.yaml` file

        :param stage: str : ...

        :param force: bool : Default False, this flag will force to move a config data to the
                stage even though it does not have any change in the data detail.

        :param retention: bool : Default True, the retention flag for start running the purge
                method after moving config file to the stage.
        """
        loading = ConfFile(
            path=join_root_with(join_path(env.ARCHIVE_PATH, self.stage_path(stage))),
            compress=self.stage_rules(stage).get('compress')
        )
        if (
                self.compare_data(
                    hash_dict(self.pick(stage=stage), exclude_keys=self.exclude_keys)
                ) > 0
                or force
        ):
            _filename: str = self.filler(f"{Stage(name=stage).format}.{self.file_extension}")
            if loading.exists(_filename):
                # TODO: generate serial number if file exists
                self._log.p_debug(
                    f"file {_filename!r} already exists in the {stage!r} stage.",
                    force=True
                )
            loading.save_stage(
                name=_filename,
                data=merge_dict(self.data, {
                    'update_time': f"{self.timestamp:{self.datetime_fmt}}",
                    'version': f"v{str(self.version())}"
                })
            )
            # Retention process after move data to the stage successful
            if retention:
                self.purge(stage=stage)
        else:
            self._log.p_debug(
                f"Config {self.name!r} can not move {self.stage!r} -> {stage!r} because "
                f"config data does not any change or does not force moving.",
                force=True
            )
        return self.switch(stage=stage)

    @property
    def archive(self) -> bool:
        """Return True if archive was set."""
        return self._cf_archive

    def archive_path(self, stage: str, filename: str) -> str:
        """Return archived path for path removing process before delete."""
        _ac_path: str = f"{stage.lower()}_{self.update_time:%Y%m%d%H%M%S}_{filename}"
        return join_root_with(join_path(env.ARCHIVE_PATH, f'.archive/{_ac_path}'))

    def purge(self, stage: Optional[str] = None) -> None:
        """Purge configuration files that match with any rules in the stage setting."""
        _stage: str = stage or self.stage
        if not (_rules := self.stage_rules(_stage)):
            return

        loading = ConfFile(
            path=join_root_with(join_path(env.ARCHIVE_PATH, self.stage_path(_stage))),
            compress=_rules.get('compress')
        )
        results: dict = self.stage_files(_stage)
        max_file: OrderFormat = max(results.items(), key=lambda x: (x[1]['parse'],))[1]['parse']
        upper_bound: Optional[OrderFormat] = None
        if (_rtt_value := _rules.get('timestamp', 0)) > 0:
            _metric: Optional[str] = _rules.get('timestamp_metric')
            upper_bound = max_file.adjust_timestamp(_rtt_value, metric=_metric)
        elif (_rtt_value := _rules.get('version', '0.0.0')) != '0.0.0':
            upper_bound = max_file.adjust_version(_rtt_value)
        if upper_bound:
            for _, data in filter(lambda x: x[1]['parse'] < upper_bound, results.items()):
                if self.archive:
                    loading.move(data['file'], self.archive_path(stage, data['file']))
                loading.remove(data['file'])

    def remove(self, stage: Optional[str] = None) -> None:
        """Remove config file from the stage storage."""
        _stage: str = stage or self.stage
        assert _stage != 'base', "the remove method should not process on the 'base' stage"
        loading = ConfFile(
            path=join_root_with(join_path(env.ARCHIVE_PATH, self.stage_path(_stage)))
        )
        results: dict = self.stage_files(_stage)
        # Remove all files from the stage.
        for _, data in results.items():
            if self.archive:
                loading.move(data['file'], self.archive_path(stage, data['file']))
            loading.remove(data['file'])

    def deploy(self, stop: Optional[str] = None) -> Registry:
        """Deploy process that move configuration data from base to final stage.

        :param stop: str : A stage name for stop when move config from base stage
                to final stage.
        """
        _base: Registry = self
        assert stop in params.stages, 'a `stop` argument should exists in `parameter.yaml` file.'
        for stage in params.stages:
            _base: Registry = _base.move(stage)
            if stop and (stage == stop):
                break
        return _base

    def filler(self, value: str, fmt: Optional[dict] = None) -> str:
        """Fill the formatter to value input argument."""
        _formatter: dict = fmt or self.formatter({
            'timestamp': self.timestamp, 'version': self.version()
        })
        for fmt_name, fmt_mapping in _formatter.items():
            # Case I: contain formatter values.
            if _searches := re.finditer(
                    rf'(?P<name>{{{fmt_name}:(?P<format>[^{{}}]+)?}})', value
            ):
                for _search in _searches:
                    value: str = value.replace(
                        f'{{{fmt_name}:{_search.groupdict()["format"]}}}',
                        self._loop_sub_fmt(search=_search, mapping=fmt_mapping, key='value')
                    )
            # Case II: does not set any formatter value or duplicate format name but does not
            # set formatter.
            if re.search(rf'(?P<name>{{{fmt_name}}})', value):
                # Get the first format value from the formatter property.
                value: str = value.replace(
                    f'{{{fmt_name}}}', fmt_mapping[list(fmt_mapping.keys())[0]]['value']
                )
        return value

    def parser(self, stage: str, value: str) -> dict:
        """Parse formatter by generator values like timestamp, version, or serial."""
        _fmt_filled, _fmt_getter = self._stage_parser(stage=stage)
        # Parse regular expression to input value
        if not (_search := re.search(rf'^{_fmt_filled}\.{self.file_extension}$', value)):
            raise ConfigArgumentError(
                'format',
                f"{value!r} does not match with the format: '^{_fmt_filled}.{self.file_extension}$'"
            )

        _searches: dict = _search.groupdict()
        _fmt_outer: dict = {}
        for name in _searches.copy():
            if name in _fmt_getter:
                _fmt_getter[name]['value']: str = _searches.pop(name)
            else:
                _fmt_outer[name]: str = _searches.pop(name)
        return _fmt_getter

    def _stage_parser(self, stage: str) -> Tuple[str, dict]:
        """Return the both of filled and getter format from the stage format value."""

        def gen_index_stm(index: int) -> str:
            """Return suffix string for duplication.

            :param index: An index value.
            """
            return f"_{str(index - 1)}" if index > 1 else ''

        _fmt_stage: str = Stage(name=stage).format
        _get_format: dict = {}
        for fmt_name, fmt_mapping in self.formatter().items():
            _index: int = 0
            if _searches := re.finditer(
                    rf'(?P<name>{{{fmt_name}:?(?P<format>[^{{}}]+)?}})',
                    _fmt_stage
            ):
                for _search in _searches:
                    _index += 1
                    _search_fmt_old: str = ''
                    if _search_fmt := _search.group('format'):
                        # Case I: contain formatter values.
                        _search_fmt_old: str = f':{_search_fmt}'
                        _search_fmt_re: str = self._loop_sub_fmt(
                            search=_search, mapping=fmt_mapping, key='regex', index=_index
                        )
                    else:
                        # Case II: does not set any formatter value.
                        _search_fmt: str = list(fmt_mapping.keys())[0]
                        _search_fmt_re: str = fmt_mapping[_search_fmt]["regex"]
                    # Replace old format value with new mapping formatter value.
                    _fmt_name_index: str = f"{fmt_name}{gen_index_stm(_index)}"
                    _fmt_stage: str = _fmt_stage.replace(
                        f'{{{fmt_name}{_search_fmt_old}}}',
                        f'(?P<{_fmt_name_index}>{_search_fmt_re})',
                        1
                    )
                    # Keep the searched format value to getter format dict.
                    _get_format[_fmt_name_index]: dict = {'fmt': _search_fmt}
        return _fmt_stage, _get_format

    def formatter(self, mapping: Optional[dict] = None) -> dict:
        """Return Generated formatter mapping values of the register object.
        Support mapping formatter,

            - compress: the compress type value
                %g  : The GZIP compression
                %-g : The GZIP compression with shortname
                %b  : The BZ2 compression
                %x  : The XZ compression
                %z  : The Zip compression

        :param mapping: Optional[dict] : Default None, the mapping of formatter standard object
                that will parse to formatter object.
        """
        _mapping: dict = mapping or {}
        _formatter: dict = {
            'compress': {
                '%g': {'value': 'gzip'},
                '%-g': {'value': 'gz'},
                '%b': {'value': 'bz2'},
                '%x': {'value': 'xz'},
                '%z': {'value': 'zip'},
            },
            'extension': {
                '%n': {'value': self.file_extension}
            }
        }
        return merge_dict(super(Register, self).formatter(mapping=_formatter), {
            'version': Version.formatter(_mapping.get('version')),
            'timestamp': Datetime.formatter(_mapping.get('timestamp'))
        })

    @staticmethod
    def _loop_sub_fmt(search: re.Match, mapping: dict, key: str, index: int = 1):
        """Loop method for find any sub-format from search input argument.

        :param search: re.Match : A Match object from searching process.

        :param mapping: dict : A formatter mapping value for getting matching key.

        :param key: str : A key value for get value from the `mapping` parameter.
        """
        assert key in {'value', 'regex'}, "the `key` argument should be 'value' or 'regex' only."
        _search_dict: dict = search.groupdict()
        _search_re: str = _search_dict["format"]
        for _fmt in re.findall(r"(%[-+!*]?\w)", _search_re):
            try:
                _fmt_replace: str = (
                    value() if isinstance((value := mapping[_fmt][key]), Callable) else value
                )
                if index > 1 and (_sr := re.search(r'\(\?P<(?P<alias_name>\w+)>', _fmt_replace)):
                    _sr_re: str = _sr.groupdict()["alias_name"]
                    _fmt_replace: str = re.sub(
                        rf"\(\?P<{_sr_re}>", rf"(?P<{_sr_re}_{str(index - 1)}>", _fmt_replace
                    )
                _search_re: str = _search_re.replace(_fmt, _fmt_replace)
            except KeyError as err:
                raise ConfigArgumentError(
                    'format', f'string formatter of {_search_dict["name"]!r} does not support '
                              f'for key {err!r} in configuration'
                ) from err
        return _search_re


__all__ = [
    'Stage',
    'Register',
    'Registry'
]
