#!/usr/bin/env python3
import asyncio
import logging

from datetime import datetime
from typing import Mapping

import kopf

from poolboy import Poolboy
from configure_kopf_logging import configure_kopf_logging
from infinite_relative_backoff import InfiniteRelativeBackoff

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

    # Preload configuration from ResourceProviders
    await ResourceProvider.preload(logger=logger)

    # Preload for matching ResourceClaim templates
    if Poolboy.operator_mode_all_in_one or Poolboy.operator_mode_manager:
        await ResourceHandle.preload(logger=logger)


@kopf.on.cleanup()
async def cleanup(logger: kopf.ObjectLogger, **_):
    await ResourceWatch.stop_all()
    await Poolboy.on_cleanup()

@kopf.on.event(Poolboy.operator_domain, Poolboy.operator_version, 'resourceproviders')
async def resource_provider_event(event: Mapping, logger: kopf.ObjectLogger, **_) -> None:
    definition = event['object']
    if event['type'] == 'DELETED':
        await ResourceProvider.unregister(name=definition['metadata']['name'], logger=logger)
    else:
        await ResourceProvider.register(definition=definition, logger=logger)

labels_arg = {
    Poolboy.ignore_label: kopf.ABSENT,
}
if Poolboy.operator_mode_resource_handler:
    labels_arg[Poolboy.resource_handler_idx_label] = str(Poolboy.resource_handler_idx)

# Only deal with ResourceClaims in manager or all-in-one mode
if Poolboy.operator_mode_all_in_one or Poolboy.operator_mode_manager:
    @kopf.on.create(
        ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
        id='resource_claim_create', labels=labels_arg,
    )
    @kopf.on.resume(
        ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
        id='resource_claim_resume', labels=labels_arg,
    )
    @kopf.on.update(
        ResourceClaim.api_group, ResourceClaim.api_version, ResourceClaim.plural,
        id='resource_claim_update', labels=labels_arg,
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
        labels=labels_arg,
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
        labels = labels_arg,
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

if Poolboy.operator_mode_manager:
    # In manager mode ResourceHandles just need to be tracked for claim binding
    # and assigned to the correct handler.
    @kopf.on.event(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
    )
    async def resource_handle_event(
        event: Mapping,
        logger: kopf.ObjectLogger,
        **_
    ) -> None:
        definition = event['object']
        if(
            event['type'] == 'DELETED' or
            'deletionTimestamp' in definition['metadata']
        ):
            await ResourceHandle.unregister(definition['metadata']['name'])
        else:
            resource_handle = await ResourceHandle.register_definition(definition)
            await resource_handle.add_resource_handler_label()

    @kopf.on.event(
        ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
    )
    async def resource_pool_event(
        event: Mapping,
        logger: kopf.ObjectLogger,
        **_
    ) -> None:
        definition = event['object']
        if(
            event['type'] == 'DELETED' or
            'deletionTimestamp' in definition['metadata']
        ):
            return
        resource_pool = ResourcePool.from_definition(definition)
        await resource_pool.add_resource_handler_label()


# In all-in-one and resource-handler mode ResourceHandles must be handled.
if(
    Poolboy.operator_mode_all_in_one or
    Poolboy.operator_mode_resource_handler
):
    @kopf.on.create(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
        id='resource_handle_create', labels=labels_arg,
    )
    @kopf.on.resume(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
        id='resource_handle_resume', labels=labels_arg,
    )
    @kopf.on.update(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
        id='resource_handle_update', labels=labels_arg,
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
        if Poolboy.operator_mode_manager:
            await resource_handle.add_resource_handler_label()
        elif (
            Poolboy.operator_mode_all_in_one or
            Poolboy.operator_mode_resource_handler
        ):
            await resource_handle.manage(logger=logger)

    @kopf.on.delete(
        ResourceHandle.api_group, ResourceHandle.api_version, ResourceHandle.plural,
        labels=labels_arg,
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
        labels = labels_arg,
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
        id='resource_pool_create', labels=labels_arg,
    )
    @kopf.on.resume(
        ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
        id='resource_pool_resume', labels=labels_arg,
    )
    @kopf.on.update(
        ResourcePool.api_group, ResourcePool.api_version, ResourcePool.plural,
        id='resource_pool_update', labels=labels_arg,
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
        labels=labels_arg,
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
        labels = labels_arg,
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
        id='resource_watch_create', labels=labels_arg,
    )
    @kopf.on.resume(
        Poolboy.operator_domain, Poolboy.operator_version, 'resourcewatches',
        id='resource_watch_resume', labels=labels_arg,
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
