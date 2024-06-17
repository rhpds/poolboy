import asyncio
import kopf
import kubernetes_asyncio
import pytimeparse

from datetime import timedelta
from typing import List, Mapping, Optional, TypeVar

import resourcehandle

from poolboy import Poolboy

ResourceHandleT = TypeVar('ResourceHandleT', bound='ResourceHandle')
ResourcePoolT = TypeVar('ResourcePoolT', bound='ResourcePool')

class ResourcePool:
    instances = {}
    lock = asyncio.Lock()

    @classmethod
    async def get(cls, name: str) -> ResourcePoolT:
        async with cls.lock:
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
        async with cls.lock:
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
    async def unregister(cls, name: str) -> Optional[ResourcePoolT]:
        async with cls.lock:
            return cls.instances.pop(name, None)

    def __init__(self,
        annotations: kopf.Annotations,
        labels: kopf.Labels,
        meta: kopf.Meta,
        name: str,
        namespace: str,
        spec: kopf.Spec,
        status: kopf.Status,
        uid: str,
    ):
        self.annotations = annotations
        self.labels = labels
        self.lock = asyncio.Lock()
        self.meta = meta
        self.name = name
        self.namespace = namespace
        self.spec = spec
        self.status = status
        self.uid = uid

    @property
    def has_lifespan(self) -> bool:
        return 'lifespan' in self.spec

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
    def metadata(self) -> Mapping:
        return self.meta

    @property
    def min_available(self) -> int:
        return self.spec.get('minAvailable', 0)

    @property
    def ref(self) -> Mapping:
        return {
            "apiVersion": Poolboy.operator_api_version,
            "kind": "ResourcePool",
            "name": self.name,
            "namespace": self.namespace,
        }

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

    def refresh(self,
        annotations: kopf.Annotations,
        labels: kopf.Labels,
        meta: kopf.Meta,
        spec: kopf.Spec,
        status: kopf.Status,
        uid: str,
    ) -> None:
        self.annotations = annotations
        self.labels = labels
        self.meta = meta
        self.spec = spec
        self.status = status
        self.uid = uid

    async def handle_delete(self, logger: kopf.ObjectLogger):
        await resourcehandle.ResourceHandle.delete_unbound_handles_for_pool(logger=logger, resource_pool=self)

    async def manage(self, logger: kopf.ObjectLogger):
        async with self.lock:
            resource_handles = await resourcehandle.ResourceHandle.get_unbound_handles_for_pool(resource_pool=self, logger=logger)
            resource_handle_deficit = self.min_available - len(resource_handles)
            if resource_handle_deficit <= 0:
                return
            for i in range(resource_handle_deficit):
                resource_handle = await resourcehandle.ResourceHandle.create_for_pool(
                    logger=logger,
                    resource_pool=self
                )
