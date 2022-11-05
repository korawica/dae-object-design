# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import re
import packaging.version as pv
from dateutil.relativedelta import relativedelta
from typing import (
    Dict,
    List,
    Tuple,
    Optional,
    Callable,
    Union,
    Any,
    TypeVar,
)
from datetime import (
    datetime,
    timedelta,
)
from functools import (
    partial,
    total_ordering,
)
from ..utils.type import (
    remove_pad,
    concat,
)
from ..errors import ConfigArgumentError


@total_ordering
class SlotLevel:
    """Slot level object for order priority."""

    __slots__ = (
        '_sl_level',
        '_sl_slot',
    )

    def __init__(self, level: int):
        """Main initialize of the slot object that define a slot list with level input
        value length of False.

        :param level: int : A level number of the slot object.
        """
        self._sl_level: int = level
        self._sl_slot: List[bool, ...] = [False] * level

    def __repr__(self):
        return f"<{self.__class__.__name__}(level={self.level})>"

    def __str__(self):
        return str(self._sl_level)

    def __hash__(self):
        return hash(self._sl_slot)

    def __eq__(self, other):
        return (
                isinstance(other, self.__class__)
                and self.value == other.value
        )

    def __lt__(self, other):
        return self.value < other.value

    @property
    def slot(self) -> list:
        """Return the slot list."""
        return self._sl_slot

    @property
    def count(self) -> int:
        """Return the counting number of True value in the slot."""
        return len(list(filter(lambda x: x is True, self._sl_slot)))

    @property
    def level(self) -> int:
        """Return level of slot."""
        return self._sl_level

    @property
    def value(self) -> int:
        """Return a sum of weighted value from a True value in any slot position."""
        return sum(map(lambda x: x[0] * int(x[1]), enumerate(self._sl_slot, start=1)))

    def update(self, numbers: Union[int, tuple]):
        """Update value in slot from False to True"""
        _number: tuple = (numbers, ) if isinstance(numbers, int) else numbers
        for num in _number:
            if num == 0:
                continue
            elif 0 <= (_num := (num - 1)) <= self._sl_level:
                self._sl_slot[_num]: bool = True
            else:
                raise ValueError(
                    f'number for update the slot level object does not in range of 0 '
                    f'and {self._sl_level}.'
                )


@total_ordering
class BaseFormatter:
    """Base formatter object for inherit to any formatter subclass that define format
    and parse method. The bese class will implement necessary properties and method
    for subclass that should implement or enhance such as `the cls.formatter()` method
    or the `cls.priorities` property.
    """

    base_fmt: str = ''

    base_attr_prefix: str = ''

    base_level: int = 1

    @classmethod
    def parse(cls, value: str, fmt: Optional[str] = None):
        """Parse string value with its format to subclass of base formatter object.

        :param value: str : A string value that match with fmt.

        :param fmt: Optional[str]: A format value will use `cls.base_fmt` if it does not
                pass from input argument.
        """
        _fmt: str = fmt or cls.base_fmt
        _fmt_regex: dict = {f: props['regex'] for f, props in cls.formatter().items()}
        for _sup_fmt in re.findall(r"(%[-+!*]?\w)", _fmt):
            _fmt: str = _fmt.replace(_sup_fmt, _fmt_regex[_sup_fmt])
        if _search := re.search(rf'^{_fmt}$', value):
            return cls(_search.groupdict())
        raise ValueError(
            f'value {value!r} does not match with format {_fmt!r}'
        )

    def format(self, fmt: str) -> str:
        """Return string value that was filled by input format pattern argument.

        :param fmt: str : A format string value for mapping with formatter.
        """
        _formatter: Dict[str, dict] = self.formatter(self.standard)
        for _sup_fmt in re.findall(r"(%[-+!*]?\w)", fmt):
            try:
                _value: Union[Callable, str] = _formatter[_sup_fmt]['value']
                fmt: str = fmt.replace(_sup_fmt, (_value() if callable(_value) else _value))
            except KeyError as err:
                raise KeyError(
                    f'the format: {_sup_fmt!r} does not support for '
                    f'{self.__class__.__name__!r}'
                ) from err
        return fmt

    def __init__(
            self,
            fmt_mapping: Optional[dict] = None
    ):
        """Main initialization get the format mapping from input argument
        and generate the necessary attributes for define the value of this
        datetime object.

        :param fmt_mapping: Optional[dict]
        """
        _fmt_mapping: dict = fmt_mapping or {}
        setattr(
            self, f"_{self.base_attr_prefix}_level",
            SlotLevel(level=self.base_level)
        )
        _level: SlotLevel = getattr(self, f"_{self.base_attr_prefix}_level")
        for name, props in self.priorities.items():
            attr: str = name.split("_", maxsplit=1)[0]
            if getattr(self, f'_{self.base_attr_prefix}_{attr}'):
                continue
            elif name.endswith('_default'):
                setattr(
                    self, f'_{self.base_attr_prefix}_{attr}',
                    props['value']()
                )
                _level.update(props['level'])
            elif name in _fmt_mapping:
                setattr(
                    self, f'_{self.base_attr_prefix}_{attr}',
                    props['value'](_fmt_mapping[name])
                )
                _level.update(props['level'])
        setattr(
            self, f"_{self.base_attr_prefix}_{self.__class__.__name__.lower()}",
            str(self.standard_value)
        )

    def __hash__(self):
        return hash(self.standard_value)

    def __str__(self):
        return self.standard_value

    def __repr__(self):
        return f"<{self.__class__.__name__}.parse('{self.standard_value}', '{self.base_fmt}')>"

    def __eq__(self, other):
        return (
                isinstance(other, self.__class__)
                and self.standard == other.standard
        )

    def __lt__(self, other) -> bool:
        return self.standard < other.standard

    @property
    def standard(self) -> Any:
        """Return standard object value that define by any subclass."""
        return getattr(self, f"_{self.base_attr_prefix}_{self.__class__.__name__.lower()}")

    @property
    def standard_value(self) -> str:
        """Return standard string value that define by any subclass."""
        raise NotImplementedError

    @property
    def level(self) -> SlotLevel:
        """Return the slot level object of any subclass."""
        return getattr(self, f"_{self.base_attr_prefix}_level")

    @property
    def priorities(self) -> Dict[str, dict]:
        raise NotImplementedError

    @staticmethod
    def formatter(value: Optional = None) -> Dict[str, dict]:
        raise NotImplementedError

    @staticmethod
    def default(value: str) -> Callable:
        """Return value function."""
        return lambda: value


class Serial(BaseFormatter):
    """Serial object for register process that implement formatter and parser.
    """

    base_fmt: str = '%n'

    base_attr_prefix: str = 'sr'

    serial_max_padding: int = 3

    serial_max_binary: int = 8

    __slots__ = (
        '_sr_number',
        '_sr_serial',
    )

    def __init__(
            self,
            fmt_mapping: Optional[dict] = None
    ):
        # Initial necessary attributes for serial metrics value.
        self._sr_number: Optional[str] = None
        super(Serial, self).__init__(fmt_mapping)
        self._sr_serial: int = int(self.standard_value)

    @property
    def standard_value(self) -> str:
        return self._sr_number

    @property
    def priorities(self) -> Dict[str, dict]:
        return {
            'number': {'value': lambda x: x, 'level': 1, },
            'number_pad': {'value': lambda x: remove_pad(x), 'level': 1, },
            'number_binary': {'value': lambda x: str(int(x, 2)), 'level': 1, },
            'number_default': {'value': self.default('0'), 'level': 0},
        }

    @staticmethod
    def formatter(serial: Optional[int] = None) -> Dict[str, dict]:
        """Generate formatter that support mapping formatter,
            %n  : Normal format
            %p  : Padding number
            %b  : Binary number

        :param serial: Optional[int]
        """
        _value: str = '' if (_sr := (serial or 0)) == 0 else str(_sr)
        return {
            '%n': {
                'value': _value,
                'regex': r'(?P<number>[0-9]*)'
            },
            '%p': {
                'value': Serial.to_padding(_value),
                'regex': rf'(?P<number_pad>[0-9]{{{str(Serial.serial_max_padding)}}})'
            },
            '%b': {
                'value': Serial.to_binary(_value),
                'regex': r'(?P<number_binary>[0-1]*)'
            }
        }

    @staticmethod
    def to_padding(value: str) -> str:
        return value.rjust(Serial.serial_max_padding, '0') if value else value

    @staticmethod
    def to_binary(value: str) -> str:
        return f"{int(value):0{str(Serial.serial_max_binary)}b}" if value else ''


MAPPING_MONTH: dict = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12',
}

MAPPING_WEEK: dict = {
    'Mon': '0',
    'Thu': '1',
    'Wed': '2',
    'Tue': '3',
    'Fri': '4',
    'Sat': '5',
    'Sun': '6',
}


class Datetime(BaseFormatter):
    """Datetime object for register process that implement formatter and parser.
    """

    base_fmt: str = '%Y-%m-%d %H:%M:%S.%f'

    base_attr_prefix: str = 'dt'

    base_level: int = 8

    __slots__ = (
        '_dt_year',
        '_dt_month',
        '_dt_week',
        '_dt_weeks',
        '_dt_day',
        '_dt_hour',
        '_dt_minute',
        '_dt_second',
        '_dt_microsecond',
        '_dt_local',
        '_dt_datetime',
    )

    def __init__(
            self,
            fmt_mapping: Optional[dict] = None
    ):
        """Main initialization get the format mapping from input argument
        and generate the necessary attributes for define the value of this
        datetime object.

        :param fmt_mapping: Optional[dict]
        """
        # Initial necessary attributes for datetime metrics value.
        self._dt_year: Optional[str] = None
        self._dt_month: Optional[str] = None
        self._dt_week: Optional[str] = None
        self._dt_weeks: Optional[str] = None
        self._dt_day: Optional[str] = None
        self._dt_hour: Optional[str] = None
        self._dt_minute: Optional[str] = None
        self._dt_second: Optional[str] = None
        self._dt_microsecond: Optional[str] = None
        self._dt_local: Optional[str] = None
        super(Datetime, self).__init__(fmt_mapping)
        self._dt_datetime: datetime = datetime.fromisoformat(self.standard_value)

    def __repr__(self):
        return f"<{self.__class__.__name__}.parse('{self.standard_value}000', '{self.base_fmt}')>"

    @property
    def standard_value(self) -> str:
        return f"{self._dt_year}-{self._dt_month}-{self._dt_day} " \
               f"{self._dt_hour}:{self._dt_minute}:{self._dt_second}.{self._dt_microsecond[:3]}"

    @property
    def iso_date(self) -> str:
        return f"{self._dt_year}-{self._dt_month}-{self._dt_day}"

    @property
    def priorities(self) -> Dict[str, dict]:
        """Priority Properties of the datetime object

        :return: Dict[str, dict]
        """
        return {
            'local': {'value': lambda x: x, 'level': 4, },
            'year': {'value': lambda x: x, 'level': 8, },
            'year_cut_pad': {'value': lambda x: f'19{x}', 'level': 8, },
            'year_cut': {'value': lambda x: f'19{x}', 'level': 8, },
            'year_default': {'value': self.default('1990'), 'level': 0, },
            'day_year': {'value': self._from_day_year, 'level': (7, 6, ), },
            'day_year_pad': {'value': self._from_day_year, 'level': (7, 6, ), },
            'month': {'value': lambda x: x.rjust(2, '0'), 'level': 7, },
            'month_pad': {'value': lambda x: x, 'level': 7, },
            'month_short': {'value': lambda x: MAPPING_MONTH[x], 'level': 7, },
            'month_full': {'value': lambda x: MAPPING_MONTH[x[:3]], 'level': 7, },
            'month_default': {'value': self.default('01'), 'level': 0, },
            'day': {'value': lambda x: x.rjust(2, '0'), 'level': 6, },
            'day_pad': {'value': lambda x: x, 'level': 6, },
            'day_default': {'value': self.default('01'), 'level': 0, },
            'week': {'value': lambda x: x, 'level': 0, },
            'week_mon': {'value': lambda x: str(int(x) % 7), 'level': 0, },
            'week_short': {'value': lambda x: MAPPING_WEEK[x], 'level': 0, },
            'week_full': {'value': lambda x: MAPPING_WEEK[x[:3]], 'level': 0, },
            'weeks_year_mon_pad': {'value': self._from_week_year_mon, 'level': (7, 6, ), },
            'weeks_year_sun_pad': {'value': self._from_week_year_sun, 'level': (7, 6, ), },
            'week_default': {
                'value': lambda: datetime.strptime(self.iso_date, '%Y-%m-%d').strftime('%w'),
                'level': 0,
            },
            'hour': {'value': lambda x: x.rjust(2, '0'), 'level': (5, 4, ), },
            'hour_pad': {'value': lambda x: x, 'level': (5, 4, ), },
            'hour_12': {
                'value': lambda x: str(int(x) + 12).rjust(2, '0') if self._dt_local == 'PM' else x.rjust(2, '0'),
                'level': 5,
            },
            'hour_12_pad': {
                'value': lambda x: str(int(x) + 12).rjust(2, '0') if self._dt_local == 'PM' else x,
                'level': 5,
            },
            'hour_default': {'value': self.default('00'), 'level': 0, },
            'minute': {'value': lambda x: x.rjust(2, '0'), 'level': 3, },
            'minute_pad': {'value': lambda x: x, 'level': 3, },
            'minute_default': {'value': self.default('00'), 'level': 0, },
            'second': {'value': lambda x: x.rjust(2, '0'), 'level': 2, },
            'second_pad': {'value': lambda x: x, 'level': 2, },
            'second_default': {'value': self.default('00'), 'level': 0, },
            'microsecond_pad': {'value': lambda x: x, 'level': 1, },
            'microsecond_default': {'value': self.default('000000'), 'level': 0, },
        }

    @staticmethod
    def formatter(dt: Optional[datetime] = None) -> Dict[str, dict]:
        """Generate formatter that support mapping formatter,
            %n  : Normal format with `%Y%m%d_%H%M%S`
            %Y  : Year with century as a decimal number.
            %y  : Year without century as a zero-padded decimal number.
            %-y : Year without century as a decimal number.
            %m  : Month as a zero-padded decimal number.
            %-m : Month as a decimal number.
            %b  : Abbreviated month name.
            %B  : Full month name.
            %a  : the abbreviated weekday name
            %A  : the full weekday name
            %w  : weekday as a decimal number, 0 as Sunday and 6 as Saturday.
            %u  : weekday as a decimal number, 1 as Monday and 7 as Sunday.
            %d  : Day of the month as a zero-padded decimal.
            %-d : Day of the month as a decimal number.
            %H  : Hour (24-hour clock) as a zero-padded decimal number.
            %-H : Hour (24-hour clock) as a decimal number.
            %I  : Hour (12-hour clock) as a zero-padded decimal number.
            %-I : Hour (12-hour clock) as a decimal number.
            %M  : minute as a zero-padded decimal number
            %-M : minute as a decimal number
            %S  : second as a zero-padded decimal number
            %-S : second as a decimal number
            %j  : day of the year as a zero-padded decimal number
            %-j : day of the year as a decimal number
            %U  : Week number of the year (Sunday as the first day of the week).
                All days in a new year preceding the first Sunday are considered
                to be in week 0.
            %W  : Week number of the year (Monday as the first day of the week).
                All days in a new year preceding the first Monday are considered
                to be in week 0.
            %p  : Locale’s AM or PM.
            %f  : Microsecond as a decimal number, zero-padded on the left.

        :param dt: Optional[datetime]
        """
        _dt: datetime = dt or datetime.now()
        return {
            '%n': {
                'value': partial(_dt.strftime, '%Y%m%d_%H%M%S'),
                'regex': r'(?P<year>\d{4})(?P<month_pad>01|02|03|04|05|06|07|08|09|10|11|12)'
                         r'(?P<day_pad>[0-3][0-9])_(?P<hour_pad>[0-2][0-9])'
                         r'(?P<minute_pad>[0-6][0-9])(?P<second_pad>[0-6][0-9])'
            },
            '%Y': {
                'value': partial(_dt.strftime, '%Y'),
                'regex': r'(?P<year>\d{4})'
            },
            '%y': {
                'value': partial(_dt.strftime, '%y'),
                'regex': r'(?P<year_cut_pad>\d{2})'
            },
            '%-y': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%y'),
                'regex': r'(?P<year_cut>\d{1,2})'
            },
            '%m': {
                'value': partial(_dt.strftime, '%m'),
                'regex': r'(?P<month_pad>01|02|03|04|05|06|07|08|09|10|11|12)'
            },
            '%-m': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%m'),
                'regex': r'(?P<month>1|2|3|4|5|6|7|8|9|10|11|12)'
            },
            '%b': {
                'value': partial(_dt.strftime, '%b'),
                'regex': r'(?P<month_short>)Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec'
            },
            '%B': {
                'value': partial(_dt.strftime, '%B'),
                'regex': r'(?P<month_full>January|February|March|April|May|June|July|'
                         r'August|September|October|November|December)'
            },
            '%a': {
                'value': partial(_dt.strftime, '%a'),
                'regex': r'(?P<week_shortname>Mon|Thu|Wed|Tue|Fri|Sat|Sun)'
            },
            '%A': {
                'value': partial(_dt.strftime, '%A'),
                'regex': r'(?P<week_fullname>Monday|Thursday|Wednesday|Tuesday|Friday|'
                         r'Saturday|Sunday)'
            },
            '%w': {
                'value': partial(_dt.strftime, '%w'),
                'regex': r'(?P<week>[0-6])'
            },
            '%u': {
                'value': partial(_dt.strftime, '%u'),
                'regex': r'(?P<week_mon>[1-7])'
            },
            '%d': {
                'value': partial(_dt.strftime, '%d'),
                'regex': r'(?P<day_pad>[0-3][0-9])'
            },
            '%-d': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%d'),
                'regex': r'(?P<day>\d{1,2})'
            },
            '%H': {
                'value': partial(_dt.strftime, '%H'),
                'regex': r'(?P<hour_pad>[0-2][0-9])'
            },
            '%-H': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%H'),
                'regex': r'(?P<hour>\d{2})'
            },
            '%I': {
                'value': partial(_dt.strftime, '%I'),
                'regex': r'(?P<hour_12_pad>00|01|02|03|04|05|06|07|08|09|10|11|12)'
            },
            '%-I': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%I'),
                'regex': r'(?P<hour_12>0|1|2|3|4|5|6|7|8|9|10|11|12)'
            },
            '%M': {
                'value': partial(_dt.strftime, '%M'),
                'regex': r'(?P<minute_pad>[0-6][0-9])'
            },
            '%-M': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%M'),
                'regex': r'(?P<minute>\d{1,2})'
            },
            '%S': {
                'value': partial(_dt.strftime, '%S'),
                'regex': r'(?P<second_pad>[0-6][0-9])'
            },
            '%-S': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%S'),
                'regex': r'(?P<second>\d{1,2})'
            },
            '%j': {
                'value': partial(_dt.strftime, '%j'),
                'regex': r'(?P<day_year_pad>[0-3][0-9][0-9])'
            },
            '%-j': {
                'value': partial(Datetime.remove_pad_dt, _dt, '%j'),
                'regex': r'(?P<day_year>\d{1,3})'
            },
            '%U': {
                'value': partial(_dt.strftime, '%U'),
                'regex': r'(?P<weeks_year_sun_pad>[0-5][0-9])'
            },
            '%W': {
                'value': partial(_dt.strftime, '%W'),
                'regex': r'(?P<weeks_year_mon_pad>[0-5][0-9])'
            },
            '%p': {
                'value': partial(_dt.strftime, '%p'),
                'regex': r'(?P<local>PM|AM)'
            },
            '%f': {
                'value': partial(_dt.strftime, '%f'),
                'regex': r'(?P<microsecond_pad>\d{6})'
            }
        }

    def _from_day_year(self, value: str) -> str:
        _this_year = datetime.strptime(self._dt_year, "%Y") + timedelta(days=int(value))
        self._dt_month = _this_year.strftime('%m')
        return _this_year.strftime('%d')

    def _from_week_year_mon(self, value: str) -> str:
        _this_week = str(((int(self._dt_week) - 1) % 7) + 1) if self._dt_week else '1'
        _this_year = datetime.strptime(f"{self._dt_year}-W{value}-{_this_week}", '%G-W%V-%u')
        self._dt_month = _this_year.strftime('%m')
        self._dt_day = _this_year.strftime('%d')
        return _this_year.strftime('%w')

    def _from_week_year_sun(self, value: str) -> str:
        _this_year = datetime.strptime(
            f"{self._dt_year}-W{value}-{self._dt_week or '0'}", '%Y-W%U-%w'
        )
        self._dt_month = _this_year.strftime('%m')
        self._dt_day = _this_year.strftime('%d')
        return _this_year.strftime('%w')

    @staticmethod
    def remove_pad_dt(_dt, fmt: str):
        return remove_pad(_dt.datetime(fmt))


class Version(BaseFormatter):
    """Version object for register process that implement formatter and parser.
    :ref:
        - The standard of versioning will align with the PEP0440
        (https://peps.python.org/pep-0440/)
        - Enhance the version object from the packaging library
        (https://packaging.pypa.io/en/latest/version.html)
    """

    base_fmt: str = '%m_%n_%c'

    base_attr_prefix: str = 'vs'

    base_level: int = 3

    __slots__ = (
        '_vs_epoch',
        '_vs_major',
        '_vs_minor',
        '_vs_micro',
        '_vs_pre',
        '_vs_post',
        '_vs_dev',
        '_vs_local',
    )

    def __init__(
            self,
            fmt_mapping: Optional[dict] = None
    ):
        """Main initialization get the format mapping from input argument
        and generate the necessary attributes for define the value of this
        version object.

        :param fmt_mapping: Optional[dict]
        """
        # Initial necessary attributes for version metrics value.
        self._vs_epoch: Optional[str] = None
        self._vs_major: Optional[str] = None
        self._vs_minor: Optional[str] = None
        self._vs_micro: Optional[str] = None
        self._vs_pre: Optional[str] = None
        self._vs_post: Optional[str] = None
        self._vs_dev: Optional[str] = None
        self._vs_local: Optional[str] = None
        super(Version, self).__init__(fmt_mapping)
        self._vs_version = pv.parse(self.standard_value)

    def __repr__(self):
        _fmt: str = '%m.%n.%c'
        if self._vs_epoch != '0':
            _fmt: str = f"%e{_fmt}"
        if self._vs_pre:
            _fmt: str = f"{_fmt}%q"
        if self._vs_post:
            _fmt: str = f"{_fmt}%p"
        if self._vs_dev:
            _fmt: str = f"{_fmt}%d"
        if self._vs_local:
            _fmt: str = f"{_fmt}%l"
        return f"<{self.__class__.__name__}.parse('{self.standard_value}', 'v{_fmt}')>"

    @property
    def standard_value(self) -> str:
        _release: str = f"v{self._vs_major}.{self._vs_minor}.{self._vs_micro}"
        if self._vs_epoch != '0':
            _release: str = f"{self._vs_epoch}!{_release}"
        if self._vs_pre:
            _release: str = f"{_release}{self._vs_pre}"
        if self._vs_post:
            _release: str = f"{_release}{self._vs_post}"
        if self._vs_dev:
            _release: str = f"{_release}{self._vs_dev}"
        if self._vs_local:
            _release: str = f"{_release}+{self._vs_local}"
        return _release

    @property
    def priorities(self) -> Dict[str, dict]:
        return {
            'epoch': {'value': lambda x: x.rstrip('!'), 'level': 3, },
            'epoch_num': {'value': lambda x: x, 'level': 3, },
            'epoch_default': {'value': self.default('0'), 'level': 0, },
            'major': {'value': lambda x: x, 'level': 3, },
            'major_default': {'value': self.default('0'), 'level': 0, },
            'minor': {'value': lambda x: x, 'level': 2, },
            'minor_default': {'value': self.default('0'), 'level': 0, },
            'micro': {'value': lambda x: x, 'level': 1, },
            'micro_default': {'value': self.default('0'), 'level': 0, },
            'pre': {'value': lambda x: self._from_prefix, 'level': 0, },
            'post': {'value': lambda x: self._from_prefix, 'level': 0, },
            'post_num': {'value': lambda x: x, 'level': 0, },
            'dev': {'value': lambda x: x, 'level': 0, },
            'local': {'value': lambda x: x.lstrip('+'), 'level': 0, },
            'local_str': {'value': lambda x: x, 'level': 0, },
        }

    @staticmethod
    def formatter(version: Optional[pv.Version] = None) -> Dict[str, dict]:
        """Generate formatter that support mapping formatter,
            %f  : full version format with `%m_%n_%c`
            %m  : major number
            %n  : minor number
            %c  : micro number
            %e  : epoch release
            %q  : pre release
            %p  : post release
            %-p : post release number
            %d  : dev release
            %l  : local release
            %-l : local release number

        :param version:
        """
        _version: pv.Version = version or pv.parse('0.0.1')
        return {
            '%f': {
                'value': f"{_version.major}_{_version.minor}_{_version.micro}",
                'regex': r'(?P<major>\d{1,3})_(?P<minor>\d{1,3})_(?P<micro>\d{1,3})',
                'level': 3,
            },
            '%m': {
                'value': str(_version.major),
                'regex': r'(?P<major>\d{1,3})',
                'level': 1,
            },
            '%n': {
                'value': str(_version.minor),
                'regex': r'(?P<minor>\d{1,3})'
            },
            '%c': {
                'value': str(_version.micro),
                'regex': r'(?P<micro>\d{1,3})'
            },
            '%e': {
                'value': f"{_version.epoch}!",
                'regex': r'(?P<epoch>[0-9]+!)'
            },
            '%-e': {
                'value': str(_version.epoch),
                'regex': r'(?P<epoch_num>[0-9]+)'
            },
            '%q': {
                'value': concat(str(x) for x in _pre) if (_pre := _version.pre) else '',
                'regex': r'(?P<pre>(a|b|c|rc|alpha|beta|pre|preview)[-_\.]?[0-9]+)'
            },
            '%p': {
                'value': str(_version.post or ''),
                'regex': r'(?P<post>(?:(post|rev|r)[-_\.]?[0-9]+)|(?:-[0-9]+))'
            },
            '%-p': {
                'value': str(_version.post or ''),
                'regex': r'(?P<post_num>[0-9]+)'
            },
            '%d': {
                'value': str(_version.dev or ''),
                'regex': r'(?P<dev>dev[-_\.]?[0-9]+)'
            },
            '%l': {
                'value': _version.local,
                'regex': r'(?P<local>\+[a-z0-9]+(?:[-_\.][a-z0-9]+)*)'
            },
            '%-l': {
                'value': _version.local,
                'regex': r'(?P<local_str>[a-z0-9]+(?:[-_\.][a-z0-9]+)*)'
            }
        }

    @staticmethod
    def _from_prefix(value: str) -> str:
        for rep, matches in {
            'a': ["alpha"],
            'b': ["beta"],
            'rc': ["c", "pre", "preview"],
            'post': ["rev", "r", "-"],
        }.items():
            for letter in matches:
                if value.startswith(letter):
                    return value.replace(letter, rep)


Formatter = TypeVar('Formatter', bound=BaseFormatter)

FORMATTER_MAP: dict = {
    'timestamp': Datetime,
    'version': Version,
    'serial': Serial,
}


class OrderFormat:
    """Order formatter object from mapping dictionary.
    """

    __slots__ = '_od_data'

    def __init__(self, mapping: dict):
        """Main initialize process of the ordering formatter object."""
        self._od_data: dict = {}
        # TODO: add merge_dict function to mapping by {'serial': ...} before for-loop process
        for name in mapping:
            _name: str = re.sub(r'(_\d+)$', '', name)
            if _name not in self._od_data:
                self._od_data[_name]: list = []
            if _name in FORMATTER_MAP:
                if isinstance(mapping[name], dict):
                    self._od_data[_name].append(FORMATTER_MAP[_name].parse(**mapping[name]))
                elif isinstance(mapping[name], BaseFormatter):
                    self._od_data[_name].append(mapping[name])
                else:
                    raise TypeError(
                        f"value of key {_name} does not support for type {type(mapping[name])}"
                    )
            else:
                self._od_data[_name].append(mapping[name]['value'])

    @property
    def data(self) -> dict:
        """Return mapping formatter data."""
        return self._od_data

    def adjust_timestamp(self, value: int, *, metric: Optional[str] = None):
        """Adjust timestamp value in the order formatter object

        :param value: int : A datetime value for this adjustment.

        :param metric: Optional[str] : A datetime metric value for subtract to the standard value.
        """
        if 'timestamp' not in self._od_data:
            raise ConfigArgumentError(
                "timestamp",
                "order file object does not have `timestamp` in name formatter"
            )
        _metric: str = metric or 'months'
        _replace: list = []
        for time_data in self._od_data['timestamp']:
            time = time_data.standard - relativedelta(**{_metric: value})
            _replace.append(FORMATTER_MAP['timestamp'].parse(**{
                'value': time.strftime('%Y%m%d %H%M%S'),
                'fmt': '%Y%m%d %H%M%S'
            }))
        self._od_data['timestamp']: list = _replace
        return self

    def adjust_version(self, value: str):
        """Adjust version value in the order formatter object

        :param value: str : A version value for this adjustment with format '%m.%n.%c'.
        """
        if 'version' not in self._od_data:
            raise ConfigArgumentError(
                'version',
                "order file object does not have `version` in name formatter"
            )
        _replace: list = []
        for version_data in self._od_data['version']:
            # `versioning` must has 3 length of tuple
            versioning: Tuple[int, ...] = version_data.standard.release
            _values: List[int, ...] = [-99 if v == '*' else int(v) for v in value.split('.')]
            _results: List[str] = []
            for _ in range(3):
                if _values[_] == 0:
                    _results.append('0')
                elif _values[_] == -99:
                    _results.append(str(versioning[_]))
                elif (major := (versioning[_] - _values[_])) < 0:
                    _results.append('0')
                else:
                    _results.append(str(major))
            _replace.append(FORMATTER_MAP['version'].parse(**{
                'value': ".".join(_results),
                'fmt': '%m.%n.%c'
            }))
        self._od_data['version']: list = _replace
        return self

    def adjust_serial(self, value: str):
        """Adjust serial value in the order formatter object"""
        if 'serial' not in self._od_data:
            raise ConfigArgumentError(
                'serial',
                "order file object does not have `serial` in name formatter"
            )
        _replace: list = []
        for serial in self._od_data['serial']:
            number = serial.standard - int(value)
            _replace.append(FORMATTER_MAP['serial'].parse(**{
                'value': (str(number) if number >= 0 else '0'),
                'fmt': '%n'
            }))
        self._od_data['serial']: list = _replace

    def __repr__(self):
        return f"<{self.__class__.__name__}(mapping={self._od_data})>"

    def __str__(self) -> str:
        return f"({', '.join([f'{k}={v}' for k, v in self._od_data.items()])})"

    def __eq__(self, other):
        return (
                isinstance(other, self.__class__)
                and self.data == other.data
        )

    def __lt__(self, other):
        return next(
            (
                self.data[name] < other.data[name]
                for name in FORMATTER_MAP
                if (name in self.data and name in other.data)
            ),
            False,
        )

    def __le__(self, other):
        return next(
            (
                self.data[name] <= other.data[name]
                for name in FORMATTER_MAP
                if (name in self.data and name in other.data)
            ),
            self.__eq__(other),
        )


__all__ = [
    'Serial',
    'Datetime',
    'Version',
    'OrderFormat',
    'Formatter'
]
