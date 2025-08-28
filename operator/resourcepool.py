import asyncio

from datetime import timedelta
from typing import List, Mapping, TypeVar
from uuid import UUID

import kopf
import pytimeparse

import resourcehandle
import resourceprovider

from kopfobject import KopfObject
from poolboy import Poolboy

ResourceHandleT = TypeVar('ResourceHandleT', bound='ResourceHandle')
ResourcePoolT = TypeVar('ResourcePoolT', bound='ResourcePool')
ResourceProviderT = TypeVar('ResourceProviderT', bound='ResourceProvider')

class ResourcePool(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourcePool"
    plural = "resourcepools"

    instances = {}
    class_lock = asyncio.Lock()

    @classmethod
    async def get(cls, name: str) -> ResourcePoolT:
        async with cls.class_lock:
            return cls.instances.get(name)

    @classmethod
    async def register(
        cls,
        annotations: kopf.Annotations,
        labels: kopf.Labels,
        meta: kopf.Meta,
        name: str,
        namespace: str,
        spec: kopf.Spec,
        status: kopf.Status,
        uid: str,
    ) -> ResourcePoolT:
        async with cls.class_lock:
            resource_pool = cls.instances.get(name)
            if resource_pool:
                resource_pool.refresh(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            else:
                resource_pool = cls(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    name = name,
                    namespace = namespace,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            resource_pool.__register()
            return resource_pool

    @classmethod
    async def unregister(cls, name: str) -> ResourcePoolT|None:
        async with cls.class_lock:
            return cls.instances.pop(name, None)

    @property
    def delete_unhealthy_resource_handles(self) -> bool:
        return self.spec.get('deleteUnhealthyResourceHandles', False)

    @property
    def has_lifespan(self) -> bool:
        return 'lifespan' in self.spec

    @property
    def has_resource_provider(self) -> bool:
        """Return whether ResourceHandles for this pool are managed by a ResourceProvider."""
        return 'provider' in self.spec

    @property
    def ignore(self) -> bool:
        """Return whether this ResourcePool should be ignored"""
        return Poolboy.ignore_label in self.labels

    @property
    def lifespan_default(self) -> int:
        return self.spec.get('lifespan', {}).get('default')

    @property
    def lifespan_maximum(self) -> int:
        return self.spec.get('lifespan', {}).get('maximum')

    @property
    def lifespan_relative_maximum(self) -> int:
        return self.spec.get('lifespan', {}).get('relativeMaximum')

    @property
    def lifespan_unclaimed(self) -> int:
        return self.spec.get('lifespan', {}).get('unclaimed')

    @property
    def lifespan_unclaimed_seconds(self) -> int:
        interval = self.lifespan_unclaimed
        if interval:
            return pytimeparse.parse(interval)

    @property
    def lifespan_unclaimed_timedelta(self):
        seconds = self.lifespan_unclaimed_seconds
        if seconds:
            return timedelta(seconds=seconds)

    @property
    def max_unready(self) -> int|None:
        return self.spec.get('maxUnready')

    @property
    def min_available(self) -> int:
        return self.spec.get('minAvailable', 0)

    @property
    def resource_handler_idx(self) -> int:
        """Label value used to select which resource handler pod should manage this ResourcePool."""
        return int(UUID(self.uid)) % Poolboy.resource_handler_count

    @property
    def resource_provider_name(self) -> str|None:
        return self.spec.get('provider', {}).get('name')

    @property
    def resources(self) -> List[Mapping]:
        return self.spec['resources']

    @property
    def vars(self) -> Mapping:
        return self.spec.get('vars', {})

    def __register(self) -> None:
        self.instances[self.name] = self

    def __unregister(self) -> None:
        self.instances.pop(self.name, None)

    async def add_resource_handler_label(self):
        if self.labels.get(Poolboy.resource_handler_idx_label) != str(self.resource_handler_idx):
            await self.merge_patch({
                "metadata": {
                    "labels": {
                        Poolboy.resource_handler_idx_label: str(self.resource_handler_idx)
                    }
                }
            })

    async def get_resource_provider(self) -> ResourceProviderT:
        """Return ResourceProvider configured to manage ResourceHandle."""
        return await resourceprovider.ResourceProvider.get(self.resource_provider_name)

    async def handle_delete(self, logger: kopf.ObjectLogger):
        await resourcehandle.ResourceHandle.delete_unbound_handles_for_pool(logger=logger, resource_pool=self)

    async def manage(self, logger: kopf.ObjectLogger):
        async with self.lock:
            resource_handles = await resourcehandle.ResourceHandle.get_unbound_handles_for_pool(resource_pool=self, logger=logger)

            available_resource_handles = []
            ready_resource_handles = []
            resource_handles_for_status = []
            for resource_handle in resource_handles:
                if self.delete_unhealthy_resource_handles and resource_handle.is_healthy == False:
                    logger.info(f"Deleting {resource_handle} in {self} due to failed health check")
                    await resource_handle.delete()
                    continue
                available_resource_handles.append(resource_handle)
                if resource_handle.is_ready:
                    ready_resource_handles.append(resource_handle)
                resource_handles_for_status.append({
                    "healthy": resource_handle.is_healthy,
                    "name": resource_handle.name,
                    "ready": resource_handle.is_ready,
                })

            resource_handle_deficit = self.min_available - len(available_resource_handles)

            if self.max_unready is not None:
                unready_count = len(available_resource_handles) - len(ready_resource_handles)
                if resource_handle_deficit > self.max_unready - unready_count:
                    resource_handle_deficit = self.max_unready - unready_count

            if resource_handle_deficit > 0:
                for i in range(resource_handle_deficit):
                    resource_handle = await resourcehandle.ResourceHandle.create_for_pool(
                        logger=logger,
                        resource_pool=self
                    )
                    resource_handles_for_status.append({
                        "name": resource_handle.name,
                    })

            patch = []
            if not self.status:
                patch.append({
                    "op": "add",
                    "path": "/status",
                    "value": {},
                })

            if self.status.get('resourceHandles') != resource_handles_for_status:
                patch.append({
                    "op": "add",
                    "path": "/status/resourceHandles",
                    "value": resource_handles_for_status,
                })

            resource_handle_count = {
                "available": len(available_resource_handles),
                "ready": len(ready_resource_handles),
            }
            if self.status.get('resourceHandleCount') != resource_handle_count:
                patch.append({
                    "op": "add",
                    "path": "/status/resourceHandleCount",
                    "value": resource_handle_count,
                })

            if patch:
                await self.json_patch_status(patch)
