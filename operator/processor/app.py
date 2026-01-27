"""
Worker application for Poolboy.

Single class that manages:
- Celery app creation and configuration
- Signal handlers (worker lifecycle, task context)
- Task routing to partitioned queues
- Async bridge for running async code in sync tasks

Note: Celery is an implementation detail, not exposed in public API.
"""

import asyncio
import os
from contextvars import ContextVar
from functools import lru_cache
from typing import TypeVar

import aiohttp
from celery import Celery, signals
from celery.utils.log import get_task_logger
from kombu import Queue
from metrics import TimerDecoratorMeta

from .config import WorkerConfig

logger = get_task_logger(__name__)
T = TypeVar("T")


# =============================================================================
# TaskRouter - Convention-based routing
# =============================================================================


class TaskRouter:
    """
    Route tasks to queues based on module naming convention.

    Convention:
    - Task module: tasks.{module}.{task_name}
    - Resource type: derived from module (resourcepool -> resource_pool)
    - Entity name: module without 'resource' prefix (resourcepool -> pool)
    - Kwargs: {entity}_name, {entity}_namespace

    Examples:
        tasks.resourcepool.create_handles -> queue: resource_pool_0
            (uses pool_name, pool_namespace from kwargs)
        tasks.resourceclaim.bind -> queue: resource_claim_2
            (uses claim_name, claim_namespace from kwargs)
        tasks.cleanup.delete_old -> queue: cleanup
            (no partitioning if PARTITION_CLEANUP not set)

    Configuration:
        Partitioning is controlled via environment variables:
        - PARTITION_RESOURCE_POOL=4  -> 4 partitions for resource_pool
        - PARTITION_RESOURCE_CLAIM=8 -> 8 partitions for resource_claim
        - (not set) -> no partitioning, uses simple queue name
    """

    def __call__(
        self, name: str, args: tuple, kwargs: dict, options: dict, task=None, **kw
    ) -> dict | None:
        """Make router callable for Celery's task_routes."""
        return self.route(name, kwargs)

    def get_entity_from_module(self, module: str) -> str:
        """
        Extract entity name from module name.

        Examples:
            resourcepool -> pool
            resourceclaim -> claim
            cleanup -> cleanup
        """
        if module.startswith("resource") and len(module) > 8:
            return module[8:]  # resourcepool -> pool
        return module

    def get_partitions(self, resource_type: str) -> int:
        """Get number of partitions for a resource type."""
        env_key = f"PARTITION_{resource_type.upper()}"
        value = os.environ.get(env_key)
        return int(value) if value else 0

    def get_queue_name(
        self, resource_type: str, resource_name: str, namespace: str, partitions: int
    ) -> str:
        """Calculate partitioned queue name using consistent hashing."""
        import hashlib

        resource_key = f"{namespace}/{resource_name}"
        hash_value = int(hashlib.md5(resource_key.encode()).hexdigest(), 16)
        partition_index = hash_value % partitions
        return f"{resource_type}_{partition_index}"

    def get_resource_type(self, module: str) -> str:
        """
        Convert module name to resource type.

        Examples:
            resourcepool -> resource_pool
            resourceclaim -> resource_claim
            cleanup -> cleanup
        """
        if module.startswith("resource") and len(module) > 8:
            return f"resource_{module[8:]}"
        return module

    def parse_task_name(self, name: str) -> tuple[str, str] | None:
        """Parse task name to extract module."""
        parts = name.split(".")
        if len(parts) >= 3 and parts[0] == "tasks":
            return parts[1], parts[2]
        return None

    def route(self, name: str, kwargs: dict) -> dict | None:
        """Route a task to appropriate queue based on convention."""
        parsed = self.parse_task_name(name)
        if not parsed:
            return None

        module, _ = parsed
        resource_type = self.get_resource_type(module)
        partitions = self.get_partitions(resource_type)

        # No partitioning configured - use default queue
        if not partitions:
            return {"queue": "default"}

        # Get resource identifier from kwargs using convention
        # Fallback to generic 'name' and 'namespace' if entity-specific not found
        entity = self.get_entity_from_module(module)
        resource_name = kwargs.get(f"{entity}_name") or kwargs.get("name")
        namespace = kwargs.get(f"{entity}_namespace") or kwargs.get(
            "namespace", "default"
        )

        if resource_name:
            queue = self.get_queue_name(
                resource_type, resource_name, namespace, partitions
            )
            return {"queue": queue}

        # No resource identifier - use default queue
        return {"queue": "default"}


# =============================================================================
# WorkerState - Process-level state management
# =============================================================================

# Task context for distributed tracing
task_context: ContextVar[str | None] = ContextVar("task_context", default=None)


class WorkerState:
    """
    Manages worker process state.

    Uses class-level attributes (like Poolboy) instead of module globals.
    Provides clear initialization and cleanup lifecycle with resilience:
    - Lazy initialization as fallback
    - Max connection age to prevent stale connections
    - Automatic reconnect on error
    """

    loop: asyncio.AbstractEventLoop | None = None
    k8s_initialized: bool = False
    initialized_at: float = 0
    MAX_CONNECTION_AGE: int = 300  # 5 minutes

    @classmethod
    def cleanup(cls, log):
        """Cleanup resources when worker process shuts down."""
        # Mark as not initialized first to prevent new tasks from starting
        cls.k8s_initialized = False

        # Cleanup distributed lock Redis client
        from distributed_lock import DistributedLock

        DistributedLock.on_cleanup()

        if cls.loop and not cls.loop.is_closed():
            from poolboy import Poolboy

            cls.loop.run_until_complete(Poolboy.on_cleanup())
            cls.loop.close()
            log.info("Worker state cleaned up")

        cls.loop = None
        cls.initialized_at = 0

    @classmethod
    def initialize(cls, log):
        """Initialize event loop and K8s client for this worker process."""
        import time

        # Initialize distributed lock Redis client
        from distributed_lock import DistributedLock

        DistributedLock.on_startup()

        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)

        from poolboy import Poolboy

        cls.loop.run_until_complete(Poolboy.on_startup(logger=log))
        cls.k8s_initialized = True
        cls.initialized_at = time.time()

    @classmethod
    def _is_connection_stale(cls) -> bool:
        """Check if connection has exceeded max age."""
        import time

        if cls.initialized_at == 0:
            return True
        elapsed = time.time() - cls.initialized_at
        return elapsed > cls.MAX_CONNECTION_AGE

    @classmethod
    def _ensure_initialized(cls):
        """Ensure connection is initialized and fresh (lazy init + max age)."""
        not_ready = not cls.k8s_initialized or cls.loop is None or cls.loop.is_closed()
        if not_ready:
            logger.warning("WorkerState not initialized, lazy init...")
            cls.initialize(logger)
        elif cls._is_connection_stale():
            logger.info("K8s connection stale, refreshing...")
            cls.cleanup(logger)
            cls.initialize(logger)

    @classmethod
    def run_async(cls, coro):
        """
        Execute async code in the worker's event loop.

        Features:
        - Lazy initialization if not ready
        - Automatic refresh if connection is stale
        - Automatic reconnect on error
        """
        cls._ensure_initialized()

        try:
            return cls.loop.run_until_complete(coro)
        except aiohttp.ClientError as e:
            # Connection error - cleanup stale connection, let Celery retry
            logger.warning(f"K8s connection error, cleaning up: {e}")
            cls.cleanup(logger)
            raise  # Celery will retry with fresh connection
        # Note: K8sApiException (404, 409, etc.) are API errors, not connection
        # errors - they propagate normally for task logic to handle


# =============================================================================
# WorkerApp
# =============================================================================


class WorkerApp(metaclass=TimerDecoratorMeta):
    """
    Worker application factory for Poolboy.

    Responsibilities:
    - Create and configure worker app from WorkerConfig dataclass
    - Setup task queues from environment variables
    - Configure task routing via TaskRouter (convention-based)
    - Connect signal handlers for worker lifecycle
    """

    def __init__(self, config: WorkerConfig | None = None):
        """
        Initialize worker application.

        Args:
            config: WorkerConfig instance. If None, creates from env vars.
        """
        self.config = config or WorkerConfig()
        self.router = TaskRouter()
        self.app = Celery("poolboy")

        self._configure_app()
        self._configure_queues()
        self._connect_signals()
        self._setup_autodiscover()

    def _configure_app(self):
        """Apply configuration from dataclass."""
        self.app.config_from_object(self.config.to_celery_config())

    def _configure_queues(self):
        """Configure task queues and routing."""
        queue_names = self._get_all_queues()
        self.app.conf.task_queues = [Queue(q) for q in queue_names]
        self.app.conf.task_default_queue = "default"
        self.app.conf.task_routes = (self.router,)

    def _get_all_queues(self) -> list[str]:
        """Generate queue names (default + partitioned)."""
        queues = ["default"]

        # Partitioned queues (e.g., 'resource_pool_0', 'resource_pool_1')
        config = self._get_partition_config()
        for resource_type, partition_count in config.items():
            for i in range(partition_count):
                queues.append(f"{resource_type}_{i}")

        return queues

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_partition_config() -> dict[str, int]:
        """Get partition configuration from environment variables."""
        resource_types = [
            "cleanup",
            "resource_claim",
            "resource_handle",
            "resource_pool",
            "resource_provider",
            "resource_watch",
        ]
        config = {}
        for resource_type in resource_types:
            env_key = f"PARTITION_{resource_type.upper()}"
            value = os.environ.get(env_key)
            if value:
                config[resource_type] = int(value)
        return config

    def _connect_signals(self):
        """Connect all signal handlers to the Celery app."""
        signals.worker_init.connect(self._on_worker_init)
        signals.worker_shutdown.connect(self._on_worker_shutdown)
        signals.worker_process_init.connect(self._on_worker_process_init)
        shutdown_signal = signals.worker_process_shutdown
        shutdown_signal.connect(self._on_worker_process_shutdown)
        signals.task_prerun.connect(self._on_task_prerun)
        signals.task_postrun.connect(self._on_task_postrun)

    @staticmethod
    def _on_worker_init(**kwargs):
        """Initialize metrics server when main worker process starts."""
        if os.environ.get("WORKER_METRICS_ENABLED", "true").lower() != "true":
            return

        from metrics import MetricsService

        port = int(os.environ.get("WORKER_METRICS_PORT", "9090"))
        MetricsService.start(port=port)
        logger.info(f"Worker metrics server started on port {port}")

    @staticmethod
    def _on_worker_shutdown(**kwargs):
        """Stop metrics server and cleanup when worker shuts down."""
        from metrics import MetricsService

        if MetricsService._server is not None:
            MetricsService.stop()
            logger.info("Worker metrics server stopped")

    @staticmethod
    def _on_worker_process_init(**kwargs):
        """Initialize event loop and K8s client when worker process starts."""
        from cache import Cache

        Cache.initialize(standalone=False)
        WorkerState.initialize(logger)

    @staticmethod
    def _on_worker_process_shutdown(**kwargs):
        """Cleanup when worker process shuts down."""
        WorkerState.cleanup(logger)

    @staticmethod
    def _on_task_prerun(task_id=None, **kwargs):
        """Set task context before execution."""
        if task_id:
            task_context.set(task_id)

    @staticmethod
    def _on_task_postrun(task_id=None, **kwargs):
        """Clear task context after execution."""
        if task_id:
            task_context.set(None)

    def _setup_autodiscover(self):
        """Configure task autodiscovery."""
        self.app.autodiscover_tasks(["tasks"])


# =============================================================================
# Module-level exports
# =============================================================================

# Create singleton and export app
worker_app = WorkerApp()
app = worker_app.app


# =============================================================================
# Beat Schedule Setup (after all tasks are discovered)
# =============================================================================


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    """
    Configure Celery Beat schedule after app is fully initialized.

    This runs after all tasks have been discovered and registered,
    avoiding circular import issues.
    """
    enabled = os.environ.get("CELERY_SCHEDULER_ENABLED", "false")
    if enabled.lower() != "true":
        return

    # Import tasks to trigger @register_schedule decorators
    import tasks  # noqa: F401
    from scheduler.scheduler import setup_beat_schedule

    sender.conf.beat_schedule = setup_beat_schedule()
    logger.info("Beat schedule configured")
