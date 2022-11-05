from .baseconf import BaseConf
from .filelibs import (
    Env,
    Json,
    Marshal,
    Pickle,
    CSV,
    CSVPipeDim,
    Yaml,
    YamlEnv,
)
from .pathlibs import (
    PathSearch,
    join_path,
    join_root_with,
    remove_file,
    get_files,
    touch,
)


class YamlConf(BaseConf):
    """YAML config object from .yaml file loader that mapping with environment
    variable.
    """
    def __init__(self, path: str):
        super(YamlConf, self).__init__(
            YamlEnv(join_root_with(path)).load()
        )


class EnvConf(BaseConf):
    """Env config object from .env file loader."""
    def __init__(self, path: str, *, update: bool = True):
        super(EnvConf, self).__init__(
            Env(join_root_with(path)).load(update=update)
        )


class AssetConf(BaseConf):
    """Asset config object from `cls.asset_extension` files loader."""
    asset_extension: str = 'yaml'

    def __init__(self, dir_path: str):
        _result: dict = {}
        _root: str = join_root_with(dir_path)
        # TODO: Dose merge yaml file with version of key? like base:15.1 and base:14.5
        for path in get_files(_root, '*'):
            if f'.{self.asset_extension}' in str(path):
                _result |= {path.stem: YamlEnv(str(path)).load()}
        super(AssetConf, self).__init__(_result)
