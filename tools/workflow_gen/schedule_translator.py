"""
schedule_translator.py — Convert PowerCenter SchedulerDef to cron expressions.

Pluggable strategy pattern: BaseScheduleTranslator, PCScheduleTranslator.

PC schedule_type values:
  ON_DEMAND   — no schedule
  RUN_ONCE    — run once at start_time
  CONTINUOUS  — run continuously (every N minutes)
  CUSTOMIZED  — custom schedule defined in raw_attributes

Raw attributes may include:
  RECURRENCEINTERVAL — interval in minutes
  STARTDAY           — day of week (SUN=1..SAT=7)
  STARTDATE          — YYYY/MM/DD
  WEEKDAY            — Monday, Tuesday, etc.
  MONTHLYDAYOFMONTH  — day of month (1-31)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from pc_extractor.models import SchedulerDef


class BaseScheduleTranslator(ABC):
    @abstractmethod
    def translate(self, scheduler: SchedulerDef) -> str:
        """Return a cron expression string, or a # TODO comment if unknown."""


class PCScheduleTranslator(BaseScheduleTranslator):
    """Translate PowerCenter SchedulerDef → cron expression."""

    # PC weekday number → cron day-of-week (0=Sun in most cron flavors)
    _PC_DAY_MAP = {
        "1": "0", "SUN": "0", "SUNDAY": "0",
        "2": "1", "MON": "1", "MONDAY": "1",
        "3": "2", "TUE": "2", "TUESDAY": "2",
        "4": "3", "WED": "3", "WEDNESDAY": "3",
        "5": "4", "THU": "4", "THURSDAY": "4",
        "6": "5", "FRI": "5", "FRIDAY": "5",
        "7": "6", "SAT": "6", "SATURDAY": "6",
    }

    def translate(self, scheduler: SchedulerDef) -> str:
        stype = (scheduler.schedule_type or "").upper().strip()

        if stype in ("ON_DEMAND", ""):
            return "# No schedule (on-demand)"

        if stype == "RUN_ONCE":
            return self._run_once(scheduler)

        if stype == "CONTINUOUS":
            return self._continuous(scheduler)

        if stype == "CUSTOMIZED":
            return self._customized(scheduler)

        raw = scheduler.schedule_type
        return f"# TODO: translate schedule: {raw}"

    def _run_once(self, scheduler: SchedulerDef) -> str:
        start = scheduler.start_time or "00:00:00"
        try:
            h, m, *_ = start.split(":")
            return f"# Run once at {start} (cron: {int(m)} {int(h)} * * *)"
        except Exception:
            return f"# TODO: translate run-once schedule: {start}"

    def _continuous(self, scheduler: SchedulerDef) -> str:
        attrs = scheduler.raw_attributes or {}
        interval = attrs.get("RECURRENCEINTERVAL", "").strip()
        if interval and interval.isdigit():
            minutes = int(interval)
            if minutes == 1:
                return "* * * * *"
            if minutes < 60:
                return f"*/{minutes} * * * *"
            hours = minutes // 60
            return f"0 */{hours} * * *"
        return "*/15 * * * *"  # default: every 15 minutes

    def _customized(self, scheduler: SchedulerDef) -> str:
        attrs = scheduler.raw_attributes or {}

        # Daily
        if attrs.get("DAYSOFWEEKRUN", "").upper() == "ALL":
            return self._daily_cron(attrs)

        # Weekly on specific day
        weekday = attrs.get("WEEKDAY", "") or attrs.get("STARTDAY", "")
        if weekday:
            day_num = self._PC_DAY_MAP.get(weekday.upper().strip(), "")
            if day_num:
                h, m = self._parse_time(attrs.get("STARTTIME", "06:00:00"))
                return f"{m} {h} * * {day_num}"

        # Monthly on specific date
        dom = attrs.get("MONTHLYDAYOFMONTH", "").strip()
        if dom and dom.isdigit():
            h, m = self._parse_time(attrs.get("STARTTIME", "06:00:00"))
            return f"{m} {h} {int(dom)} * *"

        # Interval-based
        interval = attrs.get("RECURRENCEINTERVAL", "").strip()
        if interval and interval.isdigit():
            return f"*/{interval} * * * *"

        raw = str(attrs)[:80]
        return f"# TODO: translate schedule: {raw}"

    def _daily_cron(self, attrs: dict) -> str:
        h, m = self._parse_time(attrs.get("STARTTIME", "06:00:00"))
        return f"{m} {h} * * *"

    @staticmethod
    def _parse_time(time_str: str):
        """Parse HH:MM:SS or HH:MM → (hour, minute) as ints."""
        try:
            parts = time_str.strip().split(":")
            return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
        except Exception:
            return 6, 0


def translate_schedule(scheduler: SchedulerDef, translator: Optional[BaseScheduleTranslator] = None) -> str:
    """Translate a SchedulerDef using the given translator (default: PCScheduleTranslator)."""
    t = translator or PCScheduleTranslator()
    return t.translate(scheduler)
