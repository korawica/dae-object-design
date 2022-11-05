import calendar
import enum
import datetime
import pendulum
import pytz
from dateutil import relativedelta
from functools import partial
from typing import Optional, Union, Dict, List

LOCAL_TZ: str = 'Asia/Bangkok'

DATETIME_SET: set = {
    'year',
    'month',
    'day',
    'hour',
    'minute',
    'second',
    'microsecond',
}


def get_datetime_replace(
        year: Optional[int] = None,
        month: Optional[int] = None,
) -> Dict[str, tuple]:
    _day: int = calendar.monthrange(year, month)[1] if year and month else 31
    return {
        'year': (1990, 9999),
        'month': (1, 12),
        'day': (1, _day),
        'hour': (0, 23),
        'minute': (0, 59),
        'second': (0, 59),
        'microsecond': (0, 999999)
    }


class DatetimeDim(enum.IntEnum):
    """Datetime dimension enumerations"""
    MICROSECOND = 0
    SECOND = 1
    MINUTE = 2
    HOUR = 3
    DAY = 4
    MONTH = 5
    YEAR = 6


def now(mode: Optional[str] = None, _tz: Optional[str] = None):
    _mode: str = mode or 'pendulum'
    _tz: str = _tz or LOCAL_TZ
    switcher: dict = {
        'pendulum': partial(lambda: pendulum.now(tz=_tz)),
    }
    func = switcher.get(_mode, lambda: datetime.datetime.now(pytz.timezone(_tz)))
    return func()


def get_date(fmt: str) -> Union[datetime.datetime, datetime.date, str]:
    _datetime: datetime.datetime = now()
    if fmt == 'datetime':
        return _datetime
    elif fmt == 'date':
        return _datetime.date()
    return _datetime.strftime(fmt)


def replace_date(
        dt: datetime.datetime,
        mode: str,
        reverse: bool = False
) -> datetime.datetime:
    assert mode in {'month', 'day', 'hour', 'minute', 'second', 'microsecond'}
    replace_mapping: Dict[str, tuple] = get_datetime_replace(value.year, value.month)
    return dt.replace(
        **{
            _.name.lower(): replace_mapping[_.name.lower()][int(reverse)]
            for _ in DatetimeDim if _ < DatetimeDim[mode.upper()]
        }
    )


def next_date(
        dt: datetime.datetime,
        mode: str,
        *,
        reverse: bool = False,
        next_value: int = 1
) -> datetime.datetime:
    assert mode in {'month', 'day', 'hour', 'minute', 'second', 'microsecond'}
    return dt + relativedelta.relativedelta(
        **{f'{mode}s': (-next_value if reverse else next_value)}
    )


if __name__ == '__main__':
    nowaday = now()
    print(nowaday.weekday())
    print(next_date(nowaday, 'day', next_value=2).weekday())
    _reverse = True
    print(replace_date(nowaday, mode='hour', reverse=_reverse))
    # for _ in range(12):
    #     nowaday = next_date(nowaday, mode='month', reverse=_reverse)
    #     print(nowaday)
