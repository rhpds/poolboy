"""
Distributed locking for Poolboy using Redis.

Provides process-safe locking for Celery tasks and other components.
Uses token-based locking to prevent accidental unlock by other processes.
"""

import logging
import time
import uuid
from contextlib import contextmanager
from typing import Optional

import redis
from metrics import TimerDecoratorMeta
from poolboy import Poolboy

logger = logging.getLogger(__name__)


class DistributedLockError(Exception):
    """Exception raised when distributed lock operations fail."""
    pass


class DistributedLock(metaclass=TimerDecoratorMeta):
    """
    A distributed lock implementation using Redis.

    Features:
    - Token-based ownership (prevents accidental unlock)
    - Automatic expiration to prevent deadlocks
    - Configurable timeout and retry behavior
    - Context manager support
    - Lock extension support

    Example:
        lock = DistributedLock("resource_pool:default:my-pool")
        with lock:
            # Critical section
            pass

        # Or with acquire check:
        lock = DistributedLock("resource_pool:default:my-pool", blocking=False)
        if lock.acquire():
            try:
                # Critical section
                pass
            finally:
                lock.release()
    """

    _client: Optional[redis.Redis] = None

    def __init__(
        self,
        key: str,
        timeout: int = 300,
        blocking: bool = True,
        blocking_timeout: float = 10.0,
        retry_interval: float = 0.1,
    ):
        """
        Initialize the distributed lock.

        Args:
            key: Unique identifier (prefixed with "poolboy:lock:")
            timeout: Lock expiration time (seconds)
            blocking: If True, wait for lock acquisition
            blocking_timeout: Max time to wait if blocking (seconds)
            retry_interval: Time between acquisition attempts (seconds)
        """
        # Lazy init if on_startup() wasn't called (e.g., outside worker context)
        if self._client is None:
            self.on_startup()
        self.client = self._client
        self.key = f"poolboy:lock:{key}"
        self.timeout = timeout
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.retry_interval = retry_interval
        self.token = str(uuid.uuid4())
        self._acquired = False

    def __enter__(self):
        """Context manager entry."""
        if not self.acquire():
            raise DistributedLockError(
                f"Could not acquire lock '{self.key}' within {self.blocking_timeout}s"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.release()

    def _try_acquire(self) -> bool:
        """Single attempt to acquire the lock."""
        try:
            result = self.client.set(
                self.key,
                self.token,
                nx=True,
                ex=self.timeout,
            )
            if result:
                self._acquired = True
                logger.debug(f"Acquired lock: {self.key}")
                return True
            return False
        except redis.RedisError as e:
            logger.error(f"Error acquiring lock {self.key}: {e}")
            raise DistributedLockError(f"Failed to acquire lock: {e}")

    def acquire(self, blocking: Optional[bool] = None, timeout: Optional[float] = None) -> bool:
        """
        Acquire the distributed lock.

        Args:
            blocking: Override instance blocking setting
            timeout: Override instance blocking_timeout setting

        Returns:
            True if lock was acquired, False otherwise
        """
        should_block = blocking if blocking is not None else self.blocking
        wait_timeout = timeout if timeout is not None else self.blocking_timeout

        if not should_block:
            return self._try_acquire()

        start_time = time.time()
        while (time.time() - start_time) < wait_timeout:
            if self._try_acquire():
                return True
            time.sleep(self.retry_interval)

        logger.warning(f"Failed to acquire lock {self.key} within {wait_timeout}s")
        return False

    def extend(self, additional_time: Optional[int] = None) -> bool:
        """
        Extend the lock expiration time.

        Args:
            additional_time: Additional seconds. Defaults to original timeout.

        Returns:
            True if extended, False otherwise
        """
        if not self._acquired:
            return False

        extension = additional_time or self.timeout

        try:
            current_token = self.client.get(self.key)
            if current_token == self.token:
                if self.client.expire(self.key, extension):
                    logger.debug(f"Extended lock {self.key} by {extension}s")
                    return True
            logger.warning(f"Cannot extend lock {self.key} - not owned or expired")
            self._acquired = False
            return False
        except redis.RedisError as e:
            logger.error(f"Error extending lock {self.key}: {e}")
            raise DistributedLockError(f"Failed to extend lock: {e}")

    def is_locked(self) -> bool:
        """Check if the lock is currently held (by any process)."""
        try:
            return bool(self.client.exists(self.key))
        except redis.RedisError as e:
            raise DistributedLockError(f"Failed to check lock: {e}")

    @classmethod
    def on_cleanup(cls) -> None:
        """Close Redis client. Called from worker_process_shutdown signal."""
        if cls._client is not None:
            try:
                cls._client.close()
                logger.info("DistributedLock Redis client closed")
            except redis.RedisError as e:
                logger.warning(f"Error closing Redis client: {e}")
            finally:
                cls._client = None

    @classmethod
    def on_startup(cls) -> None:
        """Initialize Redis client. Called from worker_process_init signal."""
        if cls._client is None:
            redis_url = f"{Poolboy.redis_url}/2"
            cls._client = redis.from_url(redis_url, decode_responses=True)

    @property
    def owned(self) -> bool:
        """Check if this instance owns the lock."""
        return self._acquired

    def release(self) -> bool:
        """
        Release the distributed lock.

        Returns:
            True if released, False if not owned by this instance
        """
        if not self._acquired:
            return False

        try:
            current_token = self.client.get(self.key)
            if current_token == self.token:
                self.client.delete(self.key)
                self._acquired = False
                logger.debug(f"Released lock: {self.key}")
                return True
            else:
                logger.warning(f"Cannot release lock {self.key} - token mismatch")
                self._acquired = False
                return False
        except redis.RedisError as e:
            logger.error(f"Error releasing lock {self.key}: {e}")
            raise DistributedLockError(f"Failed to release lock: {e}")


@contextmanager
def distributed_lock(
    key: str,
    timeout: int = 300,
    blocking: bool = False,
    blocking_timeout: float = 10.0,
):
    """
    Resilient context manager for distributed locking.

    Unlike DistributedLock class, this wrapper:
    - Never raises DistributedLockError on acquire failure
    - Yields acquired: bool
    - Always handles cleanup properly

    Example:
        with distributed_lock("resource_pool:ns:name") as acquired:
            if not acquired:
                raise self.retry(countdown=5)
            # Critical section

    Note: For advanced usage (e.g., lock.extend()), use DistributedLock class directly.
    """
    lock = DistributedLock(
        key=key,
        timeout=timeout,
        blocking=blocking,
        blocking_timeout=blocking_timeout,
    )

    acquired = False
    try:
        acquired = lock.acquire()
        yield acquired
    except DistributedLockError:
        yield False
    finally:
        if acquired:
            try:
                lock.release()
            except DistributedLockError:
                pass
