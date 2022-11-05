from typing import (
    Union,
    List,
    Dict,
    Optional,
)
import pytz
from datetime import datetime
from application.core.converter import CronJob
from application.errors import ScheduleArgumentError


class BaseSchedule:

    timezone: str = 'UTC'

    @classmethod
    def from_data(cls, data: dict):
        if (_cron := data.pop('cron', None)) is None:
            raise ScheduleArgumentError(
                'cron', 'this necessary key does not exists in data.'
            )
        return cls(
            cron=_cron,
            properties=data
        )

    def __init__(self, cron: str, properties: Optional[dict] = None):
        self._cron = cron
        self._properties: dict = properties or {}

    @property
    def cron(self) -> CronJob:
        return CronJob(value=self._cron)

    @property
    def properties(self) -> dict:
        return self._properties

    def schedule(self, start: str):
        _datetime: datetime = datetime.fromisoformat(start).astimezone(pytz.timezone(self.timezone))
        return self.cron.schedule(start_date=_datetime)


class BKKSchedule(BaseSchedule):

    timezone: str = 'Asia/Bangkok'


class AWSSchedule(BaseSchedule):
    ...
