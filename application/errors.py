"""
Define Errors Object for core engine
"""

from typing import Union


class BaseError(Exception):
    """Base Error Object that use for catch any errors statement of
    all step in this application
    """


class CoreBaseError(BaseError):
    """Core Base Error object"""


class ConfigNotFound(CoreBaseError):
    """Error raise for a method not found the config file or data."""


class ConfigArgumentError(CoreBaseError):
    """Error raise for a wrong configuration argument."""

    def __init__(
            self,
            argument: Union[str, tuple],
            message: str
    ):
        """Main Initialization that merge the argument and message input values
        with specific content message together like

            `__class__` with `argument`, `message`

        :param argument: Union[str, tuple]
        :param message: str
        """
        if isinstance(argument, tuple):
            _last_arg: str = str(argument[-1])
            _argument: str = (
                    ', '.join([f"{_!r}" for _ in argument[:-1]]) +
                    f', and {_last_arg!r}'
            ) if len(argument) > 1 else f"{_last_arg!r}"
        else:
            _argument: str = f'{argument!r}'
        _message: str = f"with {_argument}, {message}"
        super(ConfigArgumentError, self).__init__(_message)


class ConnectionArgumentError(ConfigArgumentError):
    """Error raise for wrong connection argument when loading or parsing"""


class CatalogArgumentError(ConfigArgumentError):
    """Error raise for wrong catalog argument when loading or parsing"""


class NodeArgumentError(ConfigArgumentError):
    """Error raise for wrong node argument when loading or parsing"""


class ScheduleArgumentError(ConfigArgumentError):
    """Error raise for wrong schedule argument when loading or parsing"""


class PipelineArgumentError(ConfigArgumentError):
    """Error raise for wrong pipeline argument when loading or parsing"""
