"""
Beat scheduler configuration builder.
"""

from datetime import timedelta
from typing import Any

from celery.schedules import crontab
from metrics import TimerDecoratorMeta

from .config_loader import deep_merge, load_schedule_config
from .registry import CRON_FIELDS_COUNT, BeatRegistry


class BeatScheduler(metaclass=TimerDecoratorMeta):
    """Manages beat schedule configuration from ConfigMap."""

    def __init__(self):
        raw_config = load_schedule_config()
        self.schedules = raw_config.get('schedules', {})

    def _build_task_config(self, task_name: str, registry_task) -> dict[str, Any]:
        """Merge registry defaults with ConfigMap overrides."""
        if registry_task.seconds is not None:
            default_schedule = {"seconds": registry_task.seconds}
        else:
            default_schedule = {"cron": registry_task.cron}

        config = {
            "enabled": registry_task.enabled,
            "schedule": default_schedule,
            "options": {},
        }

        if task_name in self.schedules:
            config = deep_merge(self.schedules[task_name], config)

        return config

    def _parse_cron(self, cron_str: str) -> crontab:
        """Parse cron string to celery crontab object."""
        parts = cron_str.strip().split()
        if len(parts) != CRON_FIELDS_COUNT:
            raise ValueError(f"Invalid cron expression: {cron_str}")

        return crontab(
            minute=parts[0],
            hour=parts[1],
            day_of_month=parts[2],
            month_of_year=parts[3],
            day_of_week=parts[4],
        )

    def _parse_schedule(self, schedule_config: dict) -> crontab | timedelta:
        """Parse schedule config to celery schedule object."""
        if "seconds" in schedule_config:
            return timedelta(seconds=schedule_config["seconds"])
        elif "cron" in schedule_config:
            return self._parse_cron(schedule_config["cron"])
        else:
            raise ValueError(f"Invalid schedule config: {schedule_config}")

    def build_schedule(self) -> dict[str, dict]:
        """Build Celery beat_schedule from registry and ConfigMap."""
        BeatRegistry.validate_registry()

        beat_schedule = {}

        for task_name, registry_task in BeatRegistry.list_all().items():
            config = self._build_task_config(task_name, registry_task)

            if not config.get("enabled", False):
                continue

            schedule_entry = {
                "task": registry_task.task_func.name,
                "schedule": self._parse_schedule(config["schedule"]),
            }

            if "options" in config:
                schedule_entry["options"] = config["options"]

            beat_schedule[task_name] = schedule_entry

        return beat_schedule


def setup_beat_schedule() -> dict[str, dict]:
    """Setup function called from processor/app.py when scheduler is enabled."""
    scheduler = BeatScheduler()
    return scheduler.build_schedule()
