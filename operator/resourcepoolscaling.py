import asyncio

from datetime import datetime, timezone
from typing import TypeVar
from uuid import UUID

import kopf
import pytimeparse

from kubernetes_asyncio.client.rest import ApiException as k8sApiException

from kopfobject import KopfObject
from poolboy import Poolboy

ResourcePoolScalingT = TypeVar('ResourcePoolScalingT', bound='ResourcePoolScaling')

class ResourcePoolScaling(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourcePoolScaling"
    plural = "resourcepoolscalings"

    @classmethod
    async def get_active_scaling_for_pool(cls, pool_name: str) -> ResourcePoolScalingT|None:
        async for scaling in ResourcePoolScaling.list(namespace=Poolboy.namespace):
            if (
                scaling.resource_pool_name == pool_name and
                scaling.is_active and
                not scaling.ignore
            ):
                return scaling
        return None

    @property
    def at_datetime(self) -> datetime:
        if 'at' in self.spec:
            return datetime.strptime(self.spec['at'], '%Y-%m-%dT%H:%M:%S%z')
        return datetime.now(timezone.utc)

    @property
    def count(self) -> int:
        return self.spec['count']

    @property
    def ignore(self) -> bool:
        """Return whether this ResourcePoolScaling should be ignored"""
        return Poolboy.ignore_label in self.labels

    @property
    def is_active(self) -> bool:
        """ResourcePoolScaling is active if scheduled time is in the past and
        count is less than current count."""
        return self.at_datetime <= datetime.now(timezone.utc) and self.count > self.current_count

    @property
    def current_count(self) -> int:
        """Count of ResourceHandles successfully created for this ResourcePoolScaling.
        This value is incremented for each ResourceHandle created and decremented for
        each unhealthy ResourceHandle removed from the pool."""
        return self.status.get('count', 0)

    @property
    def resource_pool_name(self) -> int:
        """Label value used to select which resource handler pod should manage this ResourcePool."""
        return self.spec['resourcePool']['name']

    @property
    def state(self) -> str|None:
        "Return state from status"
        return self.status.get('state')

    async def decrement_created_count(self) -> None:
        """Decrement count in status."""
        attempt = 0
        while True:
            try:
                await self.__set_status(
                    count=self.current_count - 1 if self.current_count > 1 else 0,
                )
                return
            except k8sApiException as exception:
                if exception.status != 422 or attempt > 10:
                    raise
                await self.refetch()
                attempt += 1

    async def increment_created_count(self) -> None:
        """Decrement count in status."""
        attempt = 0
        while True:
            try:
                await self.__set_status(count=self.current_count + 1)
                return
            except k8sApiException as exception:
                if exception.status != 422 or attempt > 10:
                    raise
                await self.refetch()
                attempt += 1

    async def manage(self, logger: kopf.ObjectLogger) -> None:
        from resourcepool import ResourcePool
        resource_pool = await ResourcePool.get(self.resource_pool_name)
        if 'ownerReferences' not in self.metadata:
            await self.json_patch([{
                "op": "add",
                "path": "/metadata/ownerReferences",
                "value": [{
                    "apiVersion": resource_pool.api_group_version,
                    "controller": True,
                    "kind": resource_pool.kind,
                    "name": resource_pool.name,
                    "uid": resource_pool.uid,
                }]
            }])
        if self.current_count >= self.count and self.state != 'done':
            await self.json_patch_status([{
                "op": "add",
                "path": "/status/state",
                "value": "done",
            }])

    async def __set_status(self,
        count: int,
    ) -> None:
        """Safely set status.
        Throws api exception with status 422 if resource state is out of sync."""
        state=(
            "waiting" if datetime.now(timezone.utc) < self.at_datetime else
            "scaling" if self.count < self.current_count else
            "done"
        )
        if self.status:
            patch = [{
                "op": "test",
                "path": "/status/count",
                "value": self.status.get('count'),
            }, {
                "op": "add",
                "path": "/status/count",
                "value": count,
            }, {
                "op": "add",
                "path": "/status/state",
                "value": state,
            }]
        else:
            patch = [{
                "op": "test",
                "path": "/status",
                "value": None,
            }, {
                "op": "add",
                "path": "/status",
                "value": {
                    "count": count,
                    "state": state,
                }
            }]

        await self.json_patch_status(patch)
