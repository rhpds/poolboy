#!/usr/bin/env python3
import asyncio
import logging
from typing import Mapping

import kopf
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

    # Use operator domain as finalizer
    settings.persistence.finalizer = (
        f"{Poolboy.operator_domain}/handler-{Poolboy.resource_handler_idx}" if Poolboy.operator_mode_resource_handler else
        f"{Poolboy.operator_domain}/watch-{Poolboy.resource_watch_name}" if Poolboy.operator_mode_resource_watch else
        Poolboy.operator_domain
    )

    # Store progress in status.
    settings.persistence.progress_storage = kopf.StatusProgressStorage(field='status.kopf.progress')

    # Only create events for warnings and errors
    settings.posting.level = logging.WARNING

    # Disable scanning for CustomResourceDefinitions updates
    settings.scanning.disabled = True

    # Configure logging
    configure_kopf_logging()

    await Poolboy.on_startup(logger=logger)

    if Poolboy.metrics_enabled:
        # Start metrics service
        await MetricsService.start(port=Poolboy.metrics_port)

    # Preload configuration from ResourceProviders
    await ResourceProvider.preload(logger=logger)

    # Preload for matching ResourceClaim templates
    if Poolboy.operator_mode_all_in_one or Poolboy.operator_mode_resource_handler:
        await ResourceHandle.preload(logger=logger)
    if Poolboy.operator_mode_resource_handler:
        ResourceHandle.start_watch_other()

@kopf.on.cleanup()
async def cleanup(logger: kopf.ObjectLogger, **_):
    if Poolboy.operator_mode_resource_handler:
        ResourceHandle.stop_watch_other()
    await ResourceWatch.stop_all()
    await Poolboy.on_cleanup()
    await MetricsService.stop()

@kopf.on.event(Poolboy.operator_domain, Poolboy.operator_version, 'resourceproviders')
async def resource_provider_event(event: Mapping, logger: kopf.ObjectLogger, **_) -> None:
    definition = event['object']
    if event['type'] == 'DELETED':
        await ResourceProvider.unregister(name=definition['metadata']['name'], logger=logger)
    else:
        await ResourceProvider.register(definition=definition, logger=logger)

label_selector = f"!{Poolboy.ignore_label}"
if Poolboy.operator_mode_resource_handler:
    label_selector += f",{Poolboy.resource_handler_idx_label}={Poolboy.resource_handler_idx}"

if Poolboy.operator_mode_manager:
    # In manager mode just label ResourceClaims, ResourceHandles, and ResourcePools
    # to assign the correct handler.
    @kopf.on.event(
        ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
        label_selector=label_selector,
    )
    async def label_resource_claim(
        event: Mapping,
        logger: kopf.ObjectLogger,
        **_
    ) -> None:
        definition = event['object']
        if event['type'] == 'DELETED':
            return
        resource_claim = ResourceClaim.from_definition(definition)
        await resource_claim.assign_resource_handler()

    @kopf.on.event(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
        label_selector=label_selector,
    )
    async def label_resource_handle(
        event: Mapping,
        logger: kopf.ObjectLogger,
        **_
    ) -> None:
        definition = event['object']
        if event['type'] == 'DELETED':
            return
        resource_handle = ResourceHandle.from_definition(definition)
        await resource_handle.assign_resource_handler()

    @kopf.on.event(
        ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
        label_selector=label_selector,
    )
    async def label_resource_pool(
        event: Mapping,
        logger: kopf.ObjectLogger,
        **_
    ) -> None:
        definition = event['object']
        if event['type'] == 'DELETED':
            return
        resource_pool = ResourcePool.from_definition(definition)
        await resource_pool.assign_resource_handler()

if(
    Poolboy.operator_mode_all_in_one or
    Poolboy.operator_mode_resource_handler
):
    # Resources are handled in either all-in-one or resource-handler mode.
    # The difference is only if labels are used to select which resources to handle.

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
                await resource_pool.manage(logger=logger)
                await asyncio.sleep(Poolboy.manage_pools_interval)
        except asyncio.CancelledError:
            pass

if (
    Poolboy.operator_mode_all_in_one or
    Poolboy.operator_mode_resource_watch or
    Poolboy.operator_mode_manager
):
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
        if (not Poolboy.operator_mode_resource_watch or
            Poolboy.resource_watch_name == name
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
            if Poolboy.operator_mode_manager:
                await resource_watch.create_pod(logger=logger)
            else:
                await resource_watch.start(logger=logger)
