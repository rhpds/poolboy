#!/usr/bin/env python3
import asyncio
import logging
import re
from typing import Mapping

import kopf
from cache import Cache
from configure_kopf_logging import configure_kopf_logging
from infinite_relative_backoff import InfiniteRelativeBackoff
from metrics import MetricsService
from poolboy import Poolboy
from resourceclaim import ResourceClaim
from resourcehandle import ResourceHandle
from resourcepool import ResourcePool
from resourceprovider import ResourceProvider
from resourcewatch import ResourceWatch


@kopf.on.startup()
async def startup(logger: kopf.ObjectLogger, settings: kopf.OperatorSettings, **_):
    # Store last handled configuration in status
    settings.persistence.diffbase_storage = kopf.StatusDiffBaseStorage(
        field='status.diffBase',
    )

    # Never give up from network errors
    settings.networking.error_backoffs = InfiniteRelativeBackoff()

    # Simplified finalizer - always use base domain
    settings.persistence.finalizer = Poolboy.operator_domain

    # Support deprecated finalizers for migration (covers /handler and /handler-N patterns)
    settings.persistence.deprecated_finalizer = re.compile(
        re.escape(Poolboy.operator_domain) + '/handler(-\\d+)?$'
    )

    # Store progress in status.
    settings.persistence.progress_storage = kopf.StatusProgressStorage(field='status.kopf.progress')

    # Only create events for warnings and errors
    settings.posting.level = logging.WARNING

    # Disable scanning for CustomResourceDefinitions updates
    settings.scanning.disabled = True

    # Configure logging
    configure_kopf_logging()
    # Initialize cache before any preload operations
    Cache.initialize(standalone=Poolboy.operator_mode_standalone)

    await Poolboy.on_startup(logger=logger)

    if Poolboy.metrics_enabled:
        # Start metrics service (sync but non-blocking - runs in daemon thread)
        MetricsService.start(port=Poolboy.metrics_port)

    # Preload configuration from ResourceProviders
    await ResourceProvider.preload(logger=logger)

    # Preload ResourceHandles in standalone mode (distributed mode uses workers)
    if Poolboy.operator_mode_standalone:
        await ResourceHandle.preload(logger=logger)

@kopf.on.cleanup()
async def cleanup(logger: kopf.ObjectLogger, **_):
    await ResourceWatch.stop_all()
    await Poolboy.on_cleanup()
    MetricsService.stop()

@kopf.on.event(Poolboy.operator_domain, Poolboy.operator_version, 'resourceproviders')
async def resource_provider_event(event: Mapping, logger: kopf.ObjectLogger, **_) -> None:
    definition = event['object']
    if event['type'] == 'DELETED':
        await ResourceProvider.unregister(name=definition['metadata']['name'], logger=logger)
    else:
        await ResourceProvider.register(definition=definition, logger=logger)

# Simplified label selector - just ignore resources with ignore label
label_selector = f"!{Poolboy.ignore_label}"

# Resource event handlers - always registered in both standalone and distributed modes
# In distributed mode, handlers dispatch to Celery workers

@kopf.on.create(
    ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
    label_selector=label_selector,
    id='resource_claim_create',
)
@kopf.on.resume(
    ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
    label_selector=label_selector,
    id='resource_claim_resume',
)
@kopf.on.update(
    ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
    label_selector=label_selector,
    id='resource_claim_update',
)
async def resource_claim_event(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    resource_claim = await ResourceClaim.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )

    # IMPORTANT: Only dispatch to worker if claim already has a handle.
    # Initial binding requires in-memory cache which workers don't have.
    # This ensures pool handles are correctly reused.
    if Poolboy.workers_resource_claim and resource_claim.has_resource_handle:
        from tasks.resourceclaim import dispatch_manage_claim
        dispatch_manage_claim(
            definition=resource_claim.definition,
            name=resource_claim.name,
            namespace=resource_claim.namespace,
        )
    else:
        await resource_claim.manage(logger=logger)

@kopf.on.delete(
    ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
    label_selector=label_selector,
)
async def resource_claim_delete(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    resource_claim = ResourceClaim(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )

    # Delegate to worker if enabled
    if Poolboy.workers_resource_claim:
        from tasks.resourceclaim import dispatch_delete_claim
        dispatch_delete_claim(
            definition=resource_claim.definition,
            name=resource_claim.name,
            namespace=resource_claim.namespace,
        )
    else:
        await resource_claim.handle_delete(logger=logger)

    await ResourceClaim.unregister(name=name, namespace=namespace)

@kopf.daemon(
    ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
    cancellation_timeout = 1,
    initial_delay = Poolboy.manage_handles_interval,
    label_selector=label_selector,
)
async def resource_claim_daemon(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped: kopf.DaemonStopped,
    uid: str,
    **_
):
    resource_claim = await ResourceClaim.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    try:
        while not stopped:
            description = str(resource_claim)
            resource_claim = await resource_claim.refetch()
            if not resource_claim:
                logger.info(f"{description} found deleted in daemon")
                return
            if not resource_claim.ignore:
                # Delegate to worker if enabled, daemon mode active, AND claim has handle
                # Claims without handle need operator for binding (cache-dependent)
                if (
                    Poolboy.workers_resource_claim and
                    resource_claim.has_resource_handle and
                    Poolboy.workers_resource_claim_daemon_mode in ('daemon', 'both')
                ):
                    from tasks.resourceclaim import dispatch_manage_claim
                    dispatch_manage_claim(
                        definition=resource_claim.definition,
                        name=resource_claim.name,
                        namespace=resource_claim.namespace,
                    )
                else:
                    await resource_claim.manage(logger=logger)
            await asyncio.sleep(Poolboy.manage_claims_interval)
    except asyncio.CancelledError:
        pass

@kopf.on.create(
    ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    id='resource_handle_create',
    label_selector=label_selector,
)
@kopf.on.resume(
    ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    id='resource_handle_resume',
    label_selector=label_selector,
)
@kopf.on.update(
    ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    id='resource_handle_update',
    label_selector=label_selector,
)
async def resource_handle_event(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    resource_handle = await ResourceHandle.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    if resource_handle.ignore:
        return
    if Poolboy.workers_resource_handle:
        from tasks.resourcehandle import dispatch_manage_handle
        dispatch_manage_handle(
            definition=resource_handle.definition,
            name=resource_handle.name,
            namespace=resource_handle.namespace,
        )
    else:
        await resource_handle.manage(logger=logger)

@kopf.on.delete(
    ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    label_selector=label_selector,
)
async def resource_handle_delete(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    await ResourceHandle.unregister(name)
    resource_handle = ResourceHandle(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    if resource_handle.ignore:
        return
    if Poolboy.workers_resource_handle:
        from tasks.resourcehandle import dispatch_delete_handle
        dispatch_delete_handle(
            definition=resource_handle.definition,
            name=resource_handle.name,
            namespace=resource_handle.namespace,
        )
    else:
        await resource_handle.handle_delete(logger=logger)

@kopf.daemon(
    ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    cancellation_timeout = 1,
    initial_delay = Poolboy.manage_handles_interval,
    label_selector=label_selector,
)
async def resource_handle_daemon(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped: kopf.DaemonStopped,
    uid: str,
    **_
):
    resource_handle = await ResourceHandle.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    try:
        while not stopped:
            description = str(resource_handle)
            resource_handle = await resource_handle.refetch()
            if not resource_handle:
                logger.info(f"{description} found deleted in daemon")
                return
            if not resource_handle.ignore:
                if Poolboy.workers_resource_handle:
                    if Poolboy.workers_resource_handle_daemon_mode in ('daemon', 'both'):
                        from tasks.resourcehandle import dispatch_manage_handle
                        dispatch_manage_handle(
                            definition=resource_handle.definition,
                            name=resource_handle.name,
                            namespace=resource_handle.namespace,
                        )
                else:
                    await resource_handle.manage(logger=logger)
            await asyncio.sleep(Poolboy.manage_handles_interval)
    except asyncio.CancelledError:
        pass

@kopf.on.create(
    ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
    id='resource_pool_create',
    label_selector=label_selector,
)
@kopf.on.resume(
    ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
    id='resource_pool_resume',
    label_selector=label_selector,
)
@kopf.on.update(
    ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
    id='resource_pool_update',
    label_selector=label_selector,
)
async def resource_pool_event(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    resource_pool = await ResourcePool.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    if Poolboy.workers_resource_pool:
        from tasks.resourcepool import dispatch_manage_pool
        dispatch_manage_pool(
            definition=resource_pool.definition,
            name=resource_pool.name,
            namespace=resource_pool.namespace,
        )
    else:
        await resource_pool.manage(logger=logger)

@kopf.on.delete(
    Poolboy.operator_domain, Poolboy.operator_version, 'resourcepools',
    label_selector=label_selector,
)
async def resource_pool_delete(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    await ResourcePool.unregister(name)
    resource_pool = ResourcePool(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    if Poolboy.workers_resource_pool:
        from tasks.resourcepool import dispatch_delete_pool_handles
        dispatch_delete_pool_handles(
            definition=resource_pool.definition,
            name=resource_pool.name,
            namespace=resource_pool.namespace,
        )
    else:
        await resource_pool.handle_delete(logger=logger)

@kopf.daemon(Poolboy.operator_domain, Poolboy.operator_version, 'resourcepools',
    cancellation_timeout = 1,
    initial_delay = Poolboy.manage_pools_interval,
    label_selector=label_selector,
)
async def resource_pool_daemon(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    stopped: kopf.DaemonStopped,
    uid: str,
    **_
):
    resource_pool = await ResourcePool.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    if resource_pool.ignore:
        return
    try:
        while not stopped:
            description = str(resource_pool)
            resource_pool = await resource_pool.refetch()
            if not resource_pool:
                logger.info(f"{description} found deleted in daemon")
                return

            if not resource_pool.ignore:
                if Poolboy.workers_resource_pool:
                    if Poolboy.workers_resource_pool_daemon_mode in ('daemon', 'both'):
                        from tasks.resourcepool import dispatch_manage_pool
                        dispatch_manage_pool(
                            definition=resource_pool.definition,
                            name=resource_pool.name,
                            namespace=resource_pool.namespace,
                        )
                else:
                    await resource_pool.manage(logger=logger)

            await asyncio.sleep(Poolboy.manage_pools_interval)
    except asyncio.CancelledError:
        pass

# ResourceWatch handlers - always start watch directly (no more create_pod)
@kopf.on.create(
    Poolboy.operator_domain, Poolboy.operator_version, 'resourcewatches',
    id='resource_watch_create',
    label_selector=label_selector,
)
@kopf.on.resume(
    Poolboy.operator_domain, Poolboy.operator_version, 'resourcewatches',
    id='resource_watch_resume',
    label_selector=label_selector,
)
async def resource_watch_create_or_resume(
    annotations: kopf.Annotations,
    labels: kopf.Labels,
    logger: kopf.ObjectLogger,
    meta: kopf.Meta,
    name: str,
    namespace: str,
    spec: kopf.Spec,
    status: kopf.Status,
    uid: str,
    **_
):
    resource_watch = await ResourceWatch.register(
        annotations = annotations,
        labels = labels,
        meta = meta,
        name = name,
        namespace = namespace,
        spec = spec,
        status = status,
        uid = uid,
    )
    # Always start watch directly (no more create_pod for manager mode)
    await resource_watch.start(logger=logger)
