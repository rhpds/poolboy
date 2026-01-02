"""
Worker configuration dataclass for Poolboy.

Reads all configuration from environment variables with sensible defaults.
This allows Helm to configure workers via ConfigMaps.
"""

import os
from dataclasses import dataclass, field, fields


def _env_bool(key: str, default: bool = False) -> bool:
    """Read boolean from environment variable."""
    return os.environ.get(key, str(default)).lower() == 'true'


def _env_int(key: str, default: int) -> int:
    """Read integer from environment variable."""
    return int(os.environ.get(key, default))


def _env_str(key: str, default: str) -> str:
    """Read string from environment variable."""
    return os.environ.get(key, default)


@dataclass
class WorkerConfig:
    """
    Worker configuration loaded from environment variables.

    Known fields are defined with explicit types. Additional CELERY_* env vars
    are loaded dynamically into _extras and included in to_celery_config().
    """

    # Hardcoded (never change)
    accept_content: list[str] = field(default_factory=lambda: ['json'])
    broker_connection_retry_on_startup: bool = True
    result_serializer: str = 'json'
    task_serializer: str = 'json'
    # Configurable via env vars
    broker_url: str = _env_str(
        'CELERY_BROKER_URL', 'redis://localhost:6379/0')
    result_backend: str = _env_str(
        'CELERY_RESULT_BACKEND', 'redis://localhost:6379/1')
    result_expires: int = _env_int('CELERY_RESULT_EXPIRES', 3600)
    result_extended: bool = _env_bool('CELERY_RESULT_EXTENDED', True)
    task_ack_late: bool = _env_bool('CELERY_TASK_ACK_LATE', True)
    task_default_retry_delay: int = _env_int(
        'CELERY_TASK_DEFAULT_RETRY_DELAY', 60)
    task_default_retry_delay_max: int = _env_int(
        'CELERY_TASK_DEFAULT_RETRY_DELAY_MAX', 600)
    task_reject_on_worker_lost: bool = _env_bool(
        'CELERY_TASK_REJECT_ON_WORKER_LOST', True)
    task_soft_time_limit: int = _env_int('CELERY_TASK_SOFT_TIME_LIMIT', 1740)
    task_time_limit: int = _env_int('CELERY_TASK_TIME_LIMIT', 1800)
    worker_prefetch_multiplier: int = _env_int(
        'CELERY_WORKER_PREFETCH_MULTIPLIER', 1)
    worker_send_task_events: bool = _env_bool(
        'CELERY_WORKER_SEND_TASK_EVENTS', True)
    task_send_sent_event: bool = _env_bool(
        'CELERY_TASK_SEND_SENT_EVENT', True)
    # Dynamic extras (populated in __post_init__)
    _extras: dict = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        """Load additional CELERY_* env vars not defined as fields."""
        known = {f.name for f in fields(self) if not f.name.startswith('_')}
        for key, value in os.environ.items():
            if key.startswith('CELERY_'):
                field_name = key[7:].lower()  # Remove CELERY_ prefix
                if field_name not in known:
                    self._extras[field_name] = self._parse_value(value)

    @staticmethod
    def _parse_value(value: str):
        """Parse string value to appropriate type using heuristics."""
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    def to_celery_config(self) -> dict:
        """Convert to Celery configuration dict including extras."""
        config = {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if not f.name.startswith('_')
        }
        config.update(self._extras)
        return config
