"""
Beat Registry for declarative periodic task definition.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import ClassVar, Optional

from metrics import TimerDecoratorMeta

logger = logging.getLogger(__name__)

CRON_FIELDS_COUNT = 5


@dataclass
class ScheduledTask:
    """Represents a scheduled periodic task."""
    task_name: str
    task_func: Callable
    description: str
    owner: str
    cron: Optional[str] = None
    seconds: Optional[int] = None
    tags: list[str] = field(default_factory=list)
    enabled: bool = False


class BeatRegistry(metaclass=TimerDecoratorMeta):
    """Registry for periodic tasks."""
    _tasks: ClassVar[dict[str, ScheduledTask]] = {}

    @classmethod
    def register(
        cls,
        task_name: str,
        description: str,
        owner: str,
        cron: Optional[str] = None,
        seconds: Optional[int] = None,
        tags: Optional[list[str]] = None,
        enabled: bool = False,
    ):
        """Decorator to register a periodic task."""
        def decorator(func: Callable) -> Callable:
            scheduled_task = ScheduledTask(
                task_name=task_name,
                task_func=func,
                cron=cron,
                seconds=seconds,
                description=description,
                owner=owner,
                tags=tags or [],
                enabled=enabled,
            )
            cls._tasks[task_name] = scheduled_task
            logger.info(f"Registered periodic task: {task_name}")
            return func
        return decorator

    @classmethod
    def get_task(cls, task_name: str) -> Optional[ScheduledTask]:
        """Get a registered task by name."""
        return cls._tasks.get(task_name)

    @classmethod
    def list_all(cls) -> dict[str, ScheduledTask]:
        """List all registered tasks."""
        return cls._tasks.copy()

    @classmethod
    def validate_registry(cls):
        """Validate all registered tasks."""
        errors = []
        for name, task in cls._tasks.items():
            has_cron = task.cron is not None
            has_seconds = task.seconds is not None
            if not has_cron and not has_seconds:
                errors.append(f"{name}: must have cron or seconds")
            if has_cron and has_seconds:
                errors.append(f"{name}: cannot have both cron and seconds")
            if has_cron and not cls._is_valid_cron(task.cron):
                errors.append(f"{name}: invalid cron '{task.cron}'")
            if has_seconds and task.seconds <= 0:
                errors.append(f"{name}: seconds must be positive")
            if not task.description:
                errors.append(f"{name}: missing description")
            if not task.owner:
                errors.append(f"{name}: missing owner")
        if errors:
            msg = f"Registry validation failed: {'; '.join(errors)}"
            raise ValueError(msg)
        logger.info(f"Registry validated: {len(cls._tasks)} tasks registered")

    @staticmethod
    def _is_valid_cron(cron_expr: str) -> bool:
        """Check if cron expression has valid number of fields."""
        parts = cron_expr.strip().split()
        return len(parts) == CRON_FIELDS_COUNT


register_schedule = BeatRegistry.register
