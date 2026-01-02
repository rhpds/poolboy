"""
Unified cache system for Poolboy.

Provides a single interface for caching with automatic backend selection:
- MemoryBackend: Used in standalone mode (single process)
- RedisBackend: Used in distributed mode (shared across workers)

Usage:
    from cache import Cache, CacheTag

    # Cache an object
    instance.cache_set(CacheTag.HANDLE, name, ttl=300)

    # Retrieve from cache
    instance = cls.cache_get(CacheTag.HANDLE, name)

    # Delete from cache
    cls.cache_delete(CacheTag.HANDLE, name)
"""

import fnmatch
import json
import logging
import time
from enum import Enum
from typing import Any, Optional, Protocol

import redis
from poolboy import Poolboy

logger = logging.getLogger(__name__)


class CacheTag(Enum):
    """Tags for cache key namespacing."""
    CLAIM = "claim"
    HANDLE = "handle"
    HANDLE_BOUND = "handle_bound"
    HANDLE_UNBOUND = "handle_unbound"
    POOL = "pool"
    PROVIDER = "provider"
    WATCH = "watch"
    WATCH_RESOURCE = "watch_resource"


class CacheBackend(Protocol):
    """Protocol defining cache backend interface."""

    def delete(self, key: str) -> None: ...
    def delete_pattern(self, pattern: str) -> int: ...
    def exists(self, key: str) -> bool: ...
    def get(self, key: str) -> Optional[Any]: ...
    def keys(self, pattern: str) -> list[str]: ...
    def set(self, key: str, value: Any, ttl: int) -> None: ...


class MemoryBackend:
    """In-memory cache backend for standalone mode."""

    def __init__(self):
        self._cache: dict[str, tuple[Any, float]] = {}

    def _cleanup_expired(self) -> None:
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, (_, exp) in self._cache.items() if exp <= now]
        for k in expired:
            del self._cache[k]

    def delete(self, key: str) -> None:
        """Delete a key from the cache."""
        self._cache.pop(key, None)

    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern. Returns count of deleted keys."""
        keys_to_delete = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
        for k in keys_to_delete:
            del self._cache[k]
        return len(keys_to_delete)

    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        if key not in self._cache:
            return False
        _, expires_at = self._cache[key]
        if expires_at <= time.time():
            del self._cache[key]
            return False
        return True

    def get(self, key: str) -> Optional[Any]:
        """Get value. Returns Python object directly."""
        if not self.exists(key):
            return None
        value, _ = self._cache[key]
        return value

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern."""
        self._cleanup_expired()
        return [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds. Stores Python object directly."""
        self._cleanup_expired()
        expires_at = time.time() + ttl
        self._cache[key] = (value, expires_at)


class RedisBackend:
    """Redis cache backend for distributed mode."""

    def __init__(self, url: str):
        self._client = redis.from_url(url, decode_responses=True)

    def delete(self, key: str) -> None:
        """Delete a key from Redis."""
        try:
            self._client.delete(key)
        except Exception as e:
            logger.warning(f"Redis delete failed for {key}: {e}")

    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching pattern. Returns count of deleted keys."""
        try:
            keys = self._client.keys(pattern)
            if keys:
                return self._client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Redis delete_pattern failed for {pattern}: {e}")
            return 0

    def exists(self, key: str) -> bool:
        """Check if key exists in Redis."""
        try:
            return bool(self._client.exists(key))
        except Exception as e:
            logger.warning(f"Redis exists check failed for {key}: {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """Get value. Returns deserialized dict."""
        try:
            data = self._client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.warning(f"Redis get failed for {key}: {e}")
            return None

    def keys(self, pattern: str) -> list[str]:
        """Get all keys matching pattern."""
        try:
            return self._client.keys(pattern)
        except Exception as e:
            logger.warning(f"Redis keys failed for {pattern}: {e}")
            return []

    def set(self, key: str, value: Any, ttl: int) -> None:
        """Set value with TTL in seconds. Serializes using 'definition' property if available."""
        try:
            if hasattr(value, 'definition'):
                data = json.dumps(value.definition)
            else:
                data = json.dumps(value)
            self._client.setex(key, ttl, data)
        except Exception as e:
            logger.warning(f"Redis set failed for {key}: {e}")


class CacheManager:
    """Unified cache interface with automatic backend selection."""

    _backend: Optional[CacheBackend] = None
    _initialized: bool = False

    @classmethod
    def _ensure_initialized(cls) -> None:
        """Lazy initialization of backend."""
        if cls._initialized:
            return
        cls.initialize()

    @classmethod
    def _make_key(cls, tag: CacheTag, identifier: str) -> str:
        """Build cache key from tag and identifier."""
        return f"poolboy:{tag.value}:{identifier}"

    @classmethod
    def delete(cls, tag: CacheTag, identifier: str) -> None:
        """Delete a cached value."""
        cls._ensure_initialized()
        key = cls._make_key(tag, identifier)
        cls._backend.delete(key)

    @classmethod
    def delete_by_tag(cls, tag: CacheTag) -> int:
        """Delete all cached values for a tag. Returns count of deleted keys."""
        cls._ensure_initialized()
        pattern = f"poolboy:{tag.value}:*"
        return cls._backend.delete_pattern(pattern)

    @classmethod
    def exists(cls, tag: CacheTag, identifier: str) -> bool:
        """Check if a value exists in the cache."""
        cls._ensure_initialized()
        key = cls._make_key(tag, identifier)
        return cls._backend.exists(key)

    @classmethod
    def get(cls, tag: CacheTag, identifier: str) -> Optional[Any]:
        """Get a value from the cache."""
        cls._ensure_initialized()
        key = cls._make_key(tag, identifier)
        return cls._backend.get(key)

    @classmethod
    def get_keys_by_tag(cls, tag: CacheTag) -> list[str]:
        """Get all identifiers for a given tag."""
        cls._ensure_initialized()
        pattern = f"poolboy:{tag.value}:*"
        prefix = f"poolboy:{tag.value}:"
        keys = cls._backend.keys(pattern)
        return [k[len(prefix):] for k in keys]

    @classmethod
    def initialize(cls, standalone: Optional[bool] = None) -> None:
        """
        Initialize the cache backend.

        Args:
            standalone: Force standalone mode. If None, uses Poolboy.operator_mode_standalone.
        """
        if cls._initialized:
            return

        if standalone is None:
            standalone = Poolboy.operator_mode_standalone

        if standalone:
            logger.info("Cache: Using MemoryBackend (standalone mode)")
            cls._backend = MemoryBackend()
        else:
            redis_url = f"{Poolboy.redis_url}/3"
            logger.info(f"Cache: Using RedisBackend ({redis_url})")
            try:
                cls._backend = RedisBackend(redis_url)
            except Exception as e:
                logger.warning(f"Redis connection failed, falling back to MemoryBackend: {e}")
                cls._backend = MemoryBackend()

        cls._initialized = True

    @classmethod
    def set(cls, tag: CacheTag, identifier: str, value: Any, ttl: int = 60) -> None:
        """Set a value in the cache with TTL in seconds."""
        cls._ensure_initialized()
        key = cls._make_key(tag, identifier)
        cls._backend.set(key, value, ttl)


# Module-level singleton
Cache = CacheManager
