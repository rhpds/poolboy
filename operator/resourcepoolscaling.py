import asyncio

from datetime import datetime, timezone
from typing import TypeVar
from uuid import UUID

import kopf
import pytimeparse

from kubernetes_asyncio.client.rest import ApiException as k8sApiException

from k8sobject import K8sObject
from poolboy import Poolboy

ResourcePoolScalingT = TypeVar('ResourcePoolScalingT', bound='ResourcePoolScaling')

class ResourcePoolScaling(K8sObject):
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

    async def decrement_created_count(self) -> None:
        """Decrement count in status."""
        while True:
            try:
                self.definition.setdefault('status', {})['count'] = self.current_count - 1
                if self.current_count >= self.count:
                    self.definition['state'] = 'done'

                self.definition = await Poolboy.custom_objects_api.replace_namespaced_custom_object_status(
                    body = self.definition,
                    group = self.api_group,
                    name = self.name,
                    namespace = self.namespace,
                    plural = self.plural,
                    version = self.api_version,
                )
                return
            except k8sApiException as exception:
                if exception.status != 409:
                    raise
                await self.refetch()

    async def increment_created_count(self) -> None:
        """Increment count in status."""
        while True:
            try:
                self.definition.setdefault('status', {})['count'] = self.current_count + 1
                if self.current_count >= self.count:
                    self.definition['state'] = 'done'

                self.definition = await Poolboy.custom_objects_api.replace_namespaced_custom_object_status(
                    body = self.definition,
                    group = self.api_group,
                    name = self.name,
                    namespace = self.namespace,
                    plural = self.plural,
                    version = self.api_version,
                )
                return
            except k8sApiException as exception:
                if exception.status != 409:
                    raise
                await self.refetch()
