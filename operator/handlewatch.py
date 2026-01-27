"""
HandleWatch - Event-driven watch for ResourceHandles.

Follows the proven pattern from ResourceWatch:
- Uses kubernetes_asyncio.watch.Watch() for event stream
- Handles 410 Expired and connection errors with automatic restart
- Works in both standalone and distributed modes
- Replaces the per-resource daemon with a single efficient watch

Key difference from daemons:
- Daemons: Loop every 60s per resource (N coroutines for N resources)
- HandleWatch: Single watch, event-driven processing (~instant latency)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Mapping

import kubernetes_asyncio
from poolboy import Poolboy
from resourcehandle import ResourceHandle

logger = logging.getLogger("handle_watch")


class HandleWatchRestartError(Exception):
    """Raised when watch needs to restart (e.g., 410 Expired)."""

    pass


class HandleWatchFailedError(Exception):
    """Raised when watch encounters an unrecoverable error."""

    pass


class HandleWatch:
    """Watch ResourceHandles for changes that require processing.

    This replaces the per-resource daemon with a single event-driven watch.
    When a handle changes, we check if it needs processing and either:
    - Process directly (standalone mode)
    - Dispatch to Celery workers (distributed mode)
    """

    # Singleton instance
    _instance = None
    _lock = asyncio.Lock()

    @classmethod
    async def start(cls):
        """Start the singleton HandleWatch instance."""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
                await cls._instance.initialize()
            return cls._instance

    @classmethod
    async def stop_all(cls):
        """Stop the singleton HandleWatch instance."""
        async with cls._lock:
            if cls._instance is not None:
                await cls._instance.shutdown()
                cls._instance = None

    def __init__(self):
        self.task = None
        # Cache last seen resourceVersion per handle for change detection
        self._rv_cache: dict[str, str] = {}

    async def initialize(self):
        """Start the watch loop as a background task."""
        logger.info("Starting HandleWatch")
        self.task = asyncio.create_task(self._watch_loop())

    async def shutdown(self):
        """Stop the watch loop."""
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        logger.info("HandleWatch stopped")

    async def _watch_loop(self):
        """Main watch loop with automatic restart on errors."""
        while True:
            watch_start = datetime.now(timezone.utc)
            try:
                await self._watch()
            except asyncio.CancelledError:
                logger.debug("HandleWatch cancelled")
                return
            except HandleWatchRestartError as e:
                logger.debug(f"HandleWatch restart: {e}")
                # Avoid tight restart loops
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 10:
                    await asyncio.sleep(10 - duration)
            except HandleWatchFailedError as e:
                logger.warning(f"HandleWatch failed: {e}")
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 60:
                    await asyncio.sleep(60 - duration)
            except Exception:
                logger.exception("HandleWatch exception")
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 60:
                    await asyncio.sleep(60 - duration)
            logger.debug("Restarting HandleWatch")

    async def _watch(self):
        """Stream events from Kubernetes API."""
        watch = None
        try:
            watch = kubernetes_asyncio.watch.Watch()
            # Watch ResourceHandles in operator namespace
            method = Poolboy.custom_objects_api.list_namespaced_custom_object
            kwargs = {
                "group": ResourceHandle.api_group,
                "version": ResourceHandle.api_version,
                "plural": ResourceHandle.plural,
                "namespace": Poolboy.namespace,
            }

            async for event in watch.stream(method, **kwargs):
                if not isinstance(event, Mapping):
                    raise HandleWatchFailedError(f"Unknown event: {event}")

                event_type = event["type"]
                event_obj = event["object"]

                if not isinstance(event_obj, Mapping):
                    event_obj = Poolboy.api_client.sanitize_for_serialization(event_obj)

                if event_type == "ERROR":
                    if event_obj.get("kind") == "Status":
                        reason = event_obj.get("reason", "")
                        if reason in ("Expired", "Gone"):
                            raise HandleWatchRestartError(reason.lower())
                        raise HandleWatchFailedError(
                            f"{reason} {event_obj.get('message', '')}"
                        )
                    raise HandleWatchFailedError(f"Unknown error: {event}")

                try:
                    await self._handle_event(event_type, event_obj)
                except Exception:
                    name = event_obj.get("metadata", {}).get("name", "unknown")
                    logger.exception(f"Error handling event for {name}")

        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 410:
                raise HandleWatchRestartError("410 Expired")
            raise
        finally:
            if watch:
                await watch.close()

    async def _handle_event(self, event_type: str, handle: Mapping) -> None:
        """Handle a single handle event."""
        metadata = handle.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        uid = metadata.get("uid")
        rv = metadata.get("resourceVersion")
        labels = metadata.get("labels", {})

        if not name:
            return

        cache_key = name

        # Handle deletion
        if event_type == "DELETED":
            self._rv_cache.pop(cache_key, None)
            return

        # Check if should be ignored
        if Poolboy.ignore_label in labels:
            return

        # Check if we've already processed this version
        if self._rv_cache.get(cache_key) == rv:
            return

        # Check if handle needs processing
        if not self._needs_processing(handle):
            # Update cache even if we don't process (to avoid recheck)
            self._rv_cache[cache_key] = rv
            return

        # Process the handle
        await self._process_handle(handle)
        self._rv_cache[cache_key] = rv

    def _needs_processing(self, handle: Mapping) -> bool:
        """Check if handle needs processing based on its state.

        Returns True if handle might need reconciliation.
        """
        spec = handle.get("spec", {})
        status = handle.get("status", {})

        # Check if past lifespan end
        lifespan_end = spec.get("lifespan", {}).get("end")
        if lifespan_end:
            try:
                end_dt = datetime.strptime(lifespan_end, "%Y-%m-%dT%H:%M:%S%z")
                if end_dt < datetime.now(timezone.utc):
                    return True  # Past lifespan end, needs delete
            except (ValueError, TypeError):
                pass

        # Check if bound to claim that might not exist
        if "resourceClaim" in spec:
            return True

        # Check if has resources that might need management
        if spec.get("resources"):
            return True

        return True  # Default: process it

    async def _process_handle(self, handle: Mapping) -> None:
        """Process a handle - works in both standalone and distributed modes.

        IMPORTANT: HandleWatch only processes handles that have been initialized.
        Initial setup (no status.resources) is done by Kopf on.create handler
        to avoid race conditions where both would try to create resources.

        Like ResourceWatch, this method works in both modes:
        - Standalone: calls resource_handle.manage() directly
        - Distributed: dispatches to Celery workers
        """
        metadata = handle.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        status = handle.get("status", {})

        # Only process handles that have been initialized (have status.resources)
        # Initial setup is done by Kopf on.create handler
        if "resources" not in status:
            logger.debug(
                f"HandleWatch skipping {name} - not initialized yet"
            )
            return

        # In distributed mode, dispatch to Celery
        if not Poolboy.is_standalone:
            from tasks.resourcehandle import dispatch_manage_handle

            dispatch_manage_handle(
                definition=handle,
                name=name,
                namespace=namespace,
            )
            logger.debug(f"HandleWatch dispatched {name} to worker")
        else:
            # In standalone mode, Kopf handlers also process create/update/delete.
            # HandleWatch provides backup processing for time-based operations.
            resource_handle = await ResourceHandle.register_definition(handle)
            if not resource_handle.ignore:
                await resource_handle.manage(logger=logger)
                logger.debug(f"HandleWatch processed {name} directly")
