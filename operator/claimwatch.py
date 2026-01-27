"""
ClaimWatch - Event-driven watch for ResourceClaims.

Follows the proven pattern from ResourceWatch:
- Uses kubernetes_asyncio.watch.Watch() for event stream
- Handles 410 Expired and connection errors with automatic restart
- Works in both standalone and distributed modes
- Replaces the per-resource daemon with a single efficient watch

Key difference from daemons:
- Daemons: Loop every 60s per resource (N coroutines for N resources)
- ClaimWatch: Single watch, event-driven processing (~instant latency)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Mapping

import kubernetes_asyncio
from poolboy import Poolboy
from resourceclaim import ResourceClaim

logger = logging.getLogger("claim_watch")


class ClaimWatchRestartError(Exception):
    """Raised when watch needs to restart (e.g., 410 Expired)."""

    pass


class ClaimWatchFailedError(Exception):
    """Raised when watch encounters an unrecoverable error."""

    pass


class ClaimWatch:
    """Watch ResourceClaims for changes that require processing.

    This replaces the per-resource daemon with a single event-driven watch.
    When a claim changes, we check if it needs processing and either:
    - Process directly (standalone mode)
    - Dispatch to Celery workers (distributed mode)
    """

    # Singleton instance
    _instance = None
    _lock = asyncio.Lock()

    @classmethod
    async def start(cls):
        """Start the singleton ClaimWatch instance."""
        async with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
                await cls._instance.initialize()
            return cls._instance

    @classmethod
    async def stop_all(cls):
        """Stop the singleton ClaimWatch instance."""
        async with cls._lock:
            if cls._instance is not None:
                await cls._instance.shutdown()
                cls._instance = None

    def __init__(self):
        self.task = None
        # Cache last seen resourceVersion per claim for change detection
        self._rv_cache: dict[str, str] = {}

    async def initialize(self):
        """Start the watch loop as a background task."""
        logger.info("Starting ClaimWatch")
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
        logger.info("ClaimWatch stopped")

    async def _watch_loop(self):
        """Main watch loop with automatic restart on errors."""
        while True:
            watch_start = datetime.now(timezone.utc)
            try:
                await self._watch()
            except asyncio.CancelledError:
                logger.debug("ClaimWatch cancelled")
                return
            except ClaimWatchRestartError as e:
                logger.debug(f"ClaimWatch restart: {e}")
                # Avoid tight restart loops
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 10:
                    await asyncio.sleep(10 - duration)
            except ClaimWatchFailedError as e:
                logger.warning(f"ClaimWatch failed: {e}")
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 60:
                    await asyncio.sleep(60 - duration)
            except Exception:
                logger.exception("ClaimWatch exception")
                duration = (datetime.now(timezone.utc) - watch_start).total_seconds()
                if duration < 60:
                    await asyncio.sleep(60 - duration)
            logger.debug("Restarting ClaimWatch")

    async def _watch(self):
        """Stream events from Kubernetes API."""
        watch = None
        try:
            watch = kubernetes_asyncio.watch.Watch()
            # Watch all ResourceClaims cluster-wide
            method = Poolboy.custom_objects_api.list_cluster_custom_object
            kwargs = {
                "group": ResourceClaim.api_group,
                "version": ResourceClaim.api_version,
                "plural": ResourceClaim.plural,
            }

            async for event in watch.stream(method, **kwargs):
                if not isinstance(event, Mapping):
                    raise ClaimWatchFailedError(f"Unknown event: {event}")

                event_type = event["type"]
                event_obj = event["object"]

                if not isinstance(event_obj, Mapping):
                    event_obj = Poolboy.api_client.sanitize_for_serialization(event_obj)

                if event_type == "ERROR":
                    if event_obj.get("kind") == "Status":
                        reason = event_obj.get("reason", "")
                        if reason in ("Expired", "Gone"):
                            raise ClaimWatchRestartError(reason.lower())
                        raise ClaimWatchFailedError(
                            f"{reason} {event_obj.get('message', '')}"
                        )
                    raise ClaimWatchFailedError(f"Unknown error: {event}")

                try:
                    await self._handle_event(event_type, event_obj)
                except Exception:
                    name = event_obj.get("metadata", {}).get("name", "unknown")
                    ns = event_obj.get("metadata", {}).get("namespace", "unknown")
                    logger.exception(f"Error handling event for {ns}/{name}")

        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 410:
                raise ClaimWatchRestartError("410 Expired")
            raise
        finally:
            if watch:
                await watch.close()

    async def _handle_event(self, event_type: str, claim: Mapping) -> None:
        """Handle a single claim event."""
        metadata = claim.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        uid = metadata.get("uid")
        rv = metadata.get("resourceVersion")
        labels = metadata.get("labels", {})

        if not name or not namespace:
            return

        cache_key = f"{namespace}/{name}"

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

        # Check if claim needs processing
        if not self._needs_processing(claim):
            # Update cache even if we don't process (to avoid recheck)
            self._rv_cache[cache_key] = rv
            return

        # Process the claim
        await self._process_claim(claim)
        self._rv_cache[cache_key] = rv

    def _needs_processing(self, claim: Mapping) -> bool:
        """Check if claim needs processing based on its state.

        Returns True if:
        - Claim has a handle and might need reconciliation
        - Lifespan start time has been reached
        - Status indicates processing needed
        """
        status = claim.get("status", {})
        spec = claim.get("spec", {})

        # If claim has a handle, it might need processing
        if "resourceHandle" in status:
            return True

        # Check if lifespan start is in the future
        lifespan_start = spec.get("lifespan", {}).get("start")
        if lifespan_start:
            try:
                start_dt = datetime.strptime(lifespan_start, "%Y-%m-%dT%H:%M:%S%z")
                if start_dt > datetime.now(timezone.utc):
                    # Future start - don't process yet
                    return False
            except (ValueError, TypeError):
                pass

        # If detached, check lifespan end
        if status.get("resourceHandle", {}).get("detached", False):
            lifespan_end = status.get("lifespan", {}).get("end")
            if lifespan_end:
                try:
                    end_dt = datetime.strptime(lifespan_end, "%Y-%m-%dT%H:%M:%S%z")
                    if end_dt < datetime.now(timezone.utc):
                        return True  # Past lifespan end, needs delete
                except (ValueError, TypeError):
                    pass
            return False  # Detached, no processing needed

        # Default: process it
        return True

    async def _process_claim(self, claim: Mapping) -> None:
        """Process a claim - works in both standalone and distributed modes.

        IMPORTANT: ClaimWatch only processes claims that ALREADY have a handle.
        Initial binding (no handle) is done by Kopf on.create handler to avoid
        race conditions where both would try to create a handle simultaneously.

        Like ResourceWatch, this method works in both modes:
        - Standalone: calls resource_claim.manage() directly
        - Distributed: dispatches to Celery workers
        """
        metadata = claim.get("metadata", {})
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        status = claim.get("status", {})
        has_handle = "resourceHandle" in status

        # Only process claims that already have a handle
        # Initial binding is done by Kopf on.create handler
        if not has_handle:
            logger.debug(
                f"ClaimWatch skipping {namespace}/{name} - no handle yet"
            )
            return

        # In distributed mode, dispatch to Celery
        if not Poolboy.is_standalone:
            from tasks.resourceclaim import dispatch_manage_claim

            dispatch_manage_claim(
                definition=claim,
                name=name,
                namespace=namespace,
            )
            logger.debug(f"ClaimWatch dispatched {namespace}/{name} to worker")
        else:
            # In standalone mode, Kopf handlers also process create/update/delete.
            # ClaimWatch provides backup processing for time-based operations.
            # 
            # IMPORTANT: Use register_definition which updates from the event data.
            # This ensures we have the latest data from Kubernetes.
            resource_claim = await ResourceClaim.register_definition(claim)
            if not resource_claim.ignore:
                await resource_claim.manage(logger=logger)
                logger.debug(f"ClaimWatch processed {namespace}/{name} directly")
