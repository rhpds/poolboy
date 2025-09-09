import asyncio

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, TypeVar
from uuid import UUID

import jinja2
import jsonpointer
import kopf
import kubernetes_asyncio
import pytimeparse

import poolboy_k8s
import resourceclaim
import resourcepool
import resourceprovider
import resourcewatch

from kopfobject import KopfObject
from poolboy import Poolboy
from poolboy_templating import recursive_process_template_strings, timedelta_to_str

ResourceClaimT = TypeVar('ResourceClaimT', bound='ResourceClaim')
ResourceHandleT = TypeVar('ResourceHandleT', bound='ResourceHandle')
ResourcePoolT = TypeVar('ResourcePoolT', bound='ResourcePool')
ResourceProviderT = TypeVar('ResourceProviderT', bound='ResourceProvider')

class ResourceHandleMatch:
    def __init__(self, resource_handle):
        self.resource_handle = resource_handle
        self.resource_count_difference = 0
        self.resource_name_difference_count = 0
        self.template_difference_count = 0

    def __lt__(self, cmp):
        '''Compare matches by preference'''
        if self.resource_count_difference < cmp.resource_count_difference:
            return True
        if self.resource_count_difference > cmp.resource_count_difference:
            return False

        if self.resource_name_difference_count < cmp.resource_name_difference_count:
            return True
        if self.resource_name_difference_count > cmp.resource_name_difference_count:
            return False

        if self.template_difference_count < cmp.template_difference_count:
            return True
        if self.template_difference_count > cmp.template_difference_count:
            return False

        # Prefer healthy resources to unknown health state
        if self.resource_handle.is_healthy and cmp.resource_handle.is_healthy is None:
            return True
        if self.resource_handle.is_healthy is None and cmp.resource_handle.is_healthy:
            return False

        # Prefer ready resources to unready or unknown readiness state
        if self.resource_handle.is_ready and not cmp.resource_handle.is_ready:
            return True
        if not self.resource_handle.is_ready and cmp.resource_handle.is_ready:
            return False

        # Prefer unknown readiness state to known unready state
        if self.resource_handle.is_ready is None and cmp.resource_handle.is_ready is False:
            return True
        if not self.resource_handle.is_ready is False and cmp.resource_handle.is_ready is None:
            return False

        # Prefer older matches
        return self.resource_handle.creation_timestamp < cmp.resource_handle.creation_timestamp

class ResourceHandle(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourceHandle"
    plural = "resourcehandles"

    all_instances = {}
    bound_instances = {}
    unbound_instances = {}
    class_lock = asyncio.Lock()

    @classmethod
    def __register_definition(cls, definition: Mapping) -> ResourceHandleT:
        name = definition['metadata']['name']
        resource_handle = cls.all_instances.get(name)
        if resource_handle:
            resource_handle.refresh_from_definition(definition=definition)
        else:
            resource_handle = cls(
                annotations = definition['metadata'].get('annotations', {}),
                labels = definition['metadata'].get('labels', {}),
                meta = definition['metadata'],
                name = name,
                namespace = Poolboy.namespace,
                spec = definition['spec'],
                status = definition.get('status', {}),
                uid = definition['metadata']['uid'],
            )
        resource_handle.__register()
        return resource_handle

    @classmethod
    async def bind_handle_to_claim(
        cls,
        logger: kopf.ObjectLogger,
        resource_claim: ResourceClaimT,
        resource_claim_resources: List[Mapping],
    ) -> ResourceHandleT|None:
        async with cls.class_lock:
            # Check if there is already an assigned claim
            resource_handle = cls.bound_instances.get((resource_claim.namespace, resource_claim.name))
            if resource_handle:
                if await resource_handle.refetch():
                    logger.warning(f"Rebinding {resource_handle} to {resource_claim}")
                    return resource_handle
                logger.warning(f"Deleted {resource_handle} was still in memory cache")

            claim_status_resources = resource_claim.status_resources

            # Loop through unbound instances to find best match
            matches = []
            for resource_handle in cls.unbound_instances.values():
                # Skip unhealthy
                if resource_handle.is_healthy is False:
                    continue

                # Honor explicit pool requests
                if resource_claim.resource_pool_name \
                and resource_claim.resource_pool_name != resource_handle.resource_pool_name:
                    continue

                # Do not bind to handles that are near end of lifespan
                if resource_handle.has_lifespan_end \
                and resource_handle.timedelta_to_lifespan_end.total_seconds() < 120:
                    continue

                handle_resources = resource_handle.resources
                if len(resource_claim_resources) < len(handle_resources):
                    # ResourceClaim cannot match ResourceHandle if there are more
                    # resources in the ResourceHandle than the ResourceClaim
                    continue

                match = ResourceHandleMatch(resource_handle)
                match.resource_count_difference = len(resource_claim_resources) - len(handle_resources)

                for i, handle_resource in enumerate(handle_resources):
                    claim_resource = resource_claim_resources[i]

                    # ResourceProvider must match
                    provider_name = claim_status_resources[i]['provider']['name']
                    if provider_name != handle_resource['provider']['name']:
                        match = None
                        break

                    # Check resource name match
                    claim_resource_name = claim_resource.get('name')
                    handle_resource_name = handle_resource.get('name')
                    if claim_resource_name != handle_resource_name:
                        match.resource_name_difference_count += 1

                    # Use provider to check if templates match and get list of allowed differences
                    provider = await resourceprovider.ResourceProvider.get(provider_name)
                    diff_patch = provider.check_template_match(
                        handle_resource_template = handle_resource.get('template', {}),
                        claim_resource_template = claim_resource.get('template', {}),
                    )
                    if diff_patch is None:
                        match = None
                        break
                    # Match with (possibly empty) difference list
                    match.template_difference_count += len(diff_patch)

                if match:
                    matches.append(match)

            # Bind the oldest ResourceHandle with the smallest difference score
            matches.sort()
            matched_resource_handle = None
            for match in matches:
                matched_resource_handle = match.resource_handle
                patch = [
                    {
                        "op": "add",
                        "path": "/spec/resourceClaim",
                        "value": {
                            "apiVersion": Poolboy.operator_api_version,
                            "kind": "ResourceClaim",
                            "name": resource_claim.name,
                            "namespace": resource_claim.namespace,
                        }
                    }
                ]

                # Set resource names and add any additional resources to handle
                for resource_index, claim_resource in enumerate(resource_claim_resources):
                    resource_name = resource_claim_resources[resource_index].get('name')
                    if resource_index < len(matched_resource_handle.resources):
                        handle_resource = matched_resource_handle.resources[resource_index]
                        if resource_name != handle_resource.get('name'):
                            patch.append({
                                "op": "add",
                                "path": f"/spec/resources/{resource_index}/name",
                                "value": resource_name,
                            })
                    else:
                        patch_value = {
                            "provider": resource_claim_resources[resource_index]['provider'],
                        }
                        if resource_name:
                            patch_value['name'] = resource_name
                        patch.append({
                            "op": "add",
                            "path": f"/spec/resources/{resource_index}",
                            "value": patch_value,
                        })

                # Set lifespan end from default on claim bind
                lifespan_default = matched_resource_handle.get_lifespan_default(resource_claim)
                if lifespan_default:
                    patch.append({
                        "op": "add",
                        "path": "/spec/lifespan/end",
                        "value": (
                            datetime.now(timezone.utc) + matched_resource_handle.get_lifespan_default_timedelta(resource_claim)
                        ).strftime('%FT%TZ'),
                    })

                try:
                    await matched_resource_handle.json_patch(patch)
                    matched_resource_handle.__register()
                except kubernetes_asyncio.client.exceptions.ApiException as exception:
                    if exception.status == 404:
                        logger.warning(f"Attempt to bind deleted {matched_resource_handle} to {resource_claim}")
                        matched_resource_handle.__unregister()
                        matched_resource_handle = None
                    else:
                        raise
                if matched_resource_handle:
                    logger.info(f"Bound {matched_resource_handle} to {resource_claim}")
                    break
            else:
                # No unbound resource handle matched
                return None

        if matched_resource_handle.is_from_resource_pool:
            resource_pool = await resourcepool.ResourcePool.get(matched_resource_handle.resource_pool_name)
            if resource_pool:
                await resource_pool.manage(logger=logger)
            else:
                logger.warning(
                    f"Unable to find ResourcePool {matched_resource_handle.resource_pool_name} for "
                    f"{matched_resource_handle} claimed by {resource_claim}"
                )
        return matched_resource_handle

    @classmethod
    async def create_for_claim(cls,
        logger: kopf.ObjectLogger,
        resource_claim: ResourceClaimT,
        resource_claim_resources: List[Mapping],
    ):
        definition = {
            'apiVersion': Poolboy.operator_api_version,
            'kind': 'ResourceHandle',
            'metadata': {
                'finalizers': [ Poolboy.operator_domain ],
                'generateName': 'guid-',
                'labels': {
                    Poolboy.resource_claim_name_label: resource_claim.name,
                    Poolboy.resource_claim_namespace_label: resource_claim.namespace,
                }
            },
            'spec': {
                'resourceClaim': {
                    'apiVersion': Poolboy.operator_api_version,
                    'kind': 'ResourceClaim',
                    'name': resource_claim.name,
                    'namespace': resource_claim.namespace
                },
            }
        }

        resources = []
        lifespan_default_timedelta = None
        lifespan_maximum = None
        lifespan_maximum_timedelta = None
        lifespan_relative_maximum = None
        lifespan_relative_maximum_timedelta = None
        if resource_claim.has_resource_provider:
            resource_provider = await resource_claim.get_resource_provider()
            definition['spec']['resources'] = resource_claim_resources
            definition['spec']['provider'] = resource_claim.spec['provider']
            lifespan_default_timedelta = resource_provider.get_lifespan_default_timedelta(resource_claim)
            lifespan_maximum = resource_provider.lifespan_maximum
            lifespan_maximum_timedelta = resource_provider.get_lifespan_maximum_timedelta(resource_claim)
            lifespan_relative_maximum = resource_provider.lifespan_relative_maximum
            lifespan_relative_maximum_timedelta = resource_provider.get_lifespan_maximum_timedelta(resource_claim)
        else:
            resource_providers = await resource_claim.get_resource_providers(resource_claim_resources)
            for i, claim_resource in enumerate(resource_claim_resources):
                provider = resource_providers[i]
                provider_lifespan_default_timedelta = provider.get_lifespan_default_timedelta(resource_claim)
                if provider_lifespan_default_timedelta:
                    if not lifespan_default_timedelta \
                    or provider_lifespan_default_timedelta < lifespan_default_timedelta:
                        lifespan_default_timedelta = provider_lifespan_default_timedelta

                provider_lifespan_maximum_timedelta = provider.get_lifespan_maximum_timedelta(resource_claim)
                if provider_lifespan_maximum_timedelta:
                    if not lifespan_maximum_timedelta \
                    or provider_lifespan_maximum_timedelta < lifespan_maximum_timedelta:
                        lifespan_maximum = provider.lifespan_maximum
                        lifespan_maximum_timedelta = provider_lifespan_maximum_timedelta

                provider_lifespan_relative_maximum_timedelta = provider.get_lifespan_relative_maximum_timedelta(resource_claim)
                if provider_lifespan_relative_maximum_timedelta:
                    if not lifespan_relative_maximum_timedelta \
                    or provider_lifespan_relative_maximum_timedelta < lifespan_relative_maximum_timedelta:
                        lifespan_relative_maximum = provider.lifespan_relative_maximum
                        lifespan_relative_maximum_timedelta = provider_lifespan_relative_maximum_timedelta

                resources_item = {"provider": provider.as_reference()}
                if 'name' in claim_resource:
                    resources_item['name'] = claim_resource['name']
                if 'template' in claim_resource:
                    resources_item['template'] = claim_resource['template']
                resources.append(resources_item)

            definition['spec']['resources'] = resources

        lifespan_end_datetime = None
        lifespan_start_datetime = datetime.now(timezone.utc)
        requested_lifespan_end_datetime = resource_claim.requested_lifespan_end_datetime
        if requested_lifespan_end_datetime:
            lifespan_end_datetime = requested_lifespan_end_datetime
        elif lifespan_default_timedelta:
            lifespan_end_datetime = lifespan_start_datetime + lifespan_default_timedelta
        elif lifespan_relative_maximum_timedelta:
            lifespan_end_datetime = lifespan_start_datetime + lifespan_relative_maximum_timedelta
        elif lifespan_maximum_timedelta:
            lifespan_end_datetime = lifespan_start_datetime + lifespan_maximum_timedelta

        if lifespan_end_datetime:
            if lifespan_relative_maximum_timedelta \
            and lifespan_end_datetime > lifespan_start_datetime + lifespan_relative_maximum_timedelta:
                logger.warning(
                    f"Requested lifespan end {resource_claim.requested_lifespan_end_timestamp} "
                    f"for ResourceClaim {resource_claim.name} in {resource_claim.namespace} "
                    f"exceeds relativeMaximum for ResourceProviders"
                )
                lifespan_end = lifespan_start_datetime + lifespan_relative_maximum_timedelta
            if lifespan_maximum_timedelta \
            and lifespan_end_datetime > lifespan_start_datetime + lifespan_maximum_timedelta:
                logger.warning(
                    f"Requested lifespan end {resource_claim.requested_lifespan_end_timestamp} "
                    f"for ResourceClaim {resource_claim.name} in {resource_claim.namespace} "
                    f"exceeds maximum for ResourceProviders"
                )
                lifespan_end = lifespan_start_datetime + lifespan_maximum_timedelta

        if lifespan_default_timedelta:
            definition['spec'].setdefault('lifespan', {})['default'] = timedelta_to_str(lifespan_default_timedelta)

        if lifespan_end_datetime:
            definition['spec'].setdefault('lifespan', {})['end'] = lifespan_end_datetime.strftime('%FT%TZ')

        if lifespan_maximum:
            definition['spec'].setdefault('lifespan', {})['maximum'] = lifespan_maximum

        if lifespan_relative_maximum:
            definition['spec'].setdefault('lifespan', {})['relativeMaximum'] = lifespan_relative_maximum

        definition = await Poolboy.custom_objects_api.create_namespaced_custom_object(
            body = definition,
            group = Poolboy.operator_domain,
            namespace = Poolboy.namespace,
            plural = 'resourcehandles',
            version = Poolboy.operator_version,
        )
        resource_handle = cls.from_definition(definition)
        if (
            Poolboy.operator_mode_all_in_one or (
                Poolboy.operator_mode_resource_handler and
                Poolboy.resource_handler_idx == resource_handle.resource_handler_idx
            )
        ):
            resource_handle.__register()
        logger.info(
            f"Created ResourceHandle {resource_handle.name} for "
            f"ResourceClaim {resource_claim.name} in {resource_claim.namespace}"
        )
        return resource_handle

    @classmethod
    async def create_for_pool(
        cls,
        logger: kopf.ObjectLogger,
        resource_pool: ResourcePoolT,
    ):
        definition = {
            "apiVersion": Poolboy.operator_api_version,
            "kind": "ResourceHandle",
            "metadata": {
                "generateName": "guid-",
                "labels": {
                    Poolboy.resource_pool_name_label: resource_pool.name,
                    Poolboy.resource_pool_namespace_label: resource_pool.namespace,
                },
            },
            "spec": {
                "resourcePool": resource_pool.reference,
                "vars": resource_pool.vars,
            }
        }

        if resource_pool.has_resource_provider:
            definition['spec']['provider'] = resource_pool.spec['provider']
            resource_provider = await resource_pool.get_resource_provider()
            if resource_provider.has_lifespan:
                definition['spec']['lifespan'] = {}
                if resource_provider.lifespan_default:
                    definition['spec']['lifespan']['default'] = resource_provider.lifespan_default
                if resource_provider.lifespan_maximum:
                    definition['spec']['lifespan']['maximum'] = resource_provider.lifespan_maximum
                if resource_provider.lifespan_relative_maximum:
                    definition['spec']['lifespan']['maximum'] = resource_provider.lifespan_relative_maximum
                if resource_provider.lifespan_unclaimed:
                    definition['spec']['lifespan']['end'] = (
                        datetime.now(timezone.utc) + resource_provider.lifespan_unclaimed_timedelta
                    ).strftime("%FT%TZ")
        else:
            definition['spec']['resources'] = resource_pool.resources

        if resource_pool.has_lifespan:
            definition['spec']['lifespan'] = {}
            if resource_pool.lifespan_default:
                definition['spec']['lifespan']['default'] = resource_pool.lifespan_default
            if resource_pool.lifespan_maximum:
                definition['spec']['lifespan']['maximum'] = resource_pool.lifespan_maximum
            if resource_pool.lifespan_relative_maximum:
                definition['spec']['lifespan']['relativeMaximum'] = resource_pool.lifespan_relative_maximum
            if resource_pool.lifespan_unclaimed:
                definition['spec']['lifespan']['end'] = (
                    datetime.now(timezone.utc) + resource_pool.lifespan_unclaimed_timedelta
                ).strftime("%FT%TZ")

        definition = await Poolboy.custom_objects_api.create_namespaced_custom_object(
            body = definition,
            group = Poolboy.operator_domain,
            namespace = Poolboy.namespace,
            plural = "resourcehandles",
            version = Poolboy.operator_version,
        )
        resource_handle = cls.from_definition(definition)
        if (
            Poolboy.operator_mode_all_in_one or (
                Poolboy.operator_mode_resource_handler and
                Poolboy.resource_handler_idx == resource_handle.resource_handler_idx
            )
        ):
            resource_handle.__register()
        logger.info(f"Created ResourceHandle {resource_handle.name} for ResourcePool {resource_pool.name}")
        return resource_handle

    @classmethod
    async def delete_unbound_handles_for_pool(
        cls,
        logger: kopf.ObjectLogger,
        resource_pool: ResourcePoolT,
    ) -> List[ResourceHandleT]:
        if Poolboy.operator_mode_all_in_one:
            async with cls.class_lock:
                resource_handles = []
                for resource_handle in list(cls.unbound_instances.values()):
                    if resource_handle.resource_pool_name == resource_pool.name \
                    and resource_handle.resource_pool_namespace == resource_pool.namespace:
                        logger.info(
                            f"Deleting unbound ResourceHandle {resource_handle.name} "
                            f"for ResourcePool {resource_pool.name}"
                        )
                        resource_handle.__unregister()
                        await resource_handle.delete()
                return resource_handles

        resource_handles = await cls.get_unbound_handles_for_pool(
            resource_pool=resource_pool,
            logger=logger,
        )
        for resource_handle in resource_handles:
            logger.info(
                f"Deleting unbound ResourceHandle {resource_handle.name} "
                f"for ResourcePool {resource_pool.name}"
            )
            await resource_handle.delete()
        return resource_handles

    @classmethod
    async def get(cls, name: str, ignore_deleting=True, use_cache=True) -> ResourceHandleT|None:
        async with cls.class_lock:
            if use_cache and name in cls.all_instances:
                return cls.all_instances[name]

            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                group=Poolboy.operator_domain,
                name=name,
                namespace=Poolboy.namespace,
                plural='resourcehandles',
                version=Poolboy.operator_version,
            )
            if ignore_deleting and 'deletionTimestamp' in definition['metadata']:
                return None
            if use_cache:
                return cls.__register_definition(definition)
            return cls.from_definition(definition)

    @classmethod
    def get_from_cache(cls, name: str) -> ResourceHandleT|None:
        return cls.all_instances.get(name)

    @classmethod
    async def get_unbound_handles_for_pool(
        cls,
        resource_pool: ResourcePoolT,
        logger: kopf.ObjectLogger,
    ) -> List[ResourceHandleT]:
        resource_handles = []
        if Poolboy.operator_mode_all_in_one:
            async with cls.class_lock:
                for resource_handle in ResourceHandle.unbound_instances.values():
                    if resource_handle.resource_pool_name == resource_pool.name \
                    and resource_handle.resource_pool_namespace == resource_pool.namespace:
                        resource_handles.append(resource_handle)
                return resource_handles

        _continue = None
        while True:
            resource_handle_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
                group=Poolboy.operator_domain,
                label_selector=f"{Poolboy.resource_pool_name_label}={resource_pool.name},!{Poolboy.resource_claim_name_label}",
                namespace=Poolboy.namespace,
                plural='resourcehandles',
                version=Poolboy.operator_version,
                _continue = _continue,
                limit = 50,
            )
            for definition in resource_handle_list['items']:
                resource_handle = cls.from_definition(definition)
                if not resource_handle.is_bound:
                    resource_handles.append(resource_handle)
            _continue = resource_handle_list['metadata'].get('continue')
            if not _continue:
                break
        return resource_handles

    @classmethod
    async def preload(cls, logger: kopf.ObjectLogger) -> None:
        async with cls.class_lock:
            _continue = None
            while True:
                resource_handle_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
                    group=Poolboy.operator_domain,
                    namespace=Poolboy.namespace,
                    plural='resourcehandles',
                    version=Poolboy.operator_version,
                    _continue = _continue,
                    limit = 50,
                )
                for definition in resource_handle_list['items']:
                    cls.__register_definition(definition=definition)
                _continue = resource_handle_list['metadata'].get('continue')
                if not _continue:
                    break

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
    ) -> ResourceHandleT:
        async with cls.class_lock:
            resource_handle = cls.all_instances.get(name)
            if resource_handle:
                resource_handle.refresh(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            else:
                resource_handle = cls(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    name = name,
                    namespace = namespace,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            resource_handle.__register()
            return resource_handle

    @classmethod
    async def register_definition(cls, definition: Mapping) -> ResourceHandleT:
        async with cls.class_lock:
            return cls.__register_definition(definition)

    @classmethod
    async def unregister(cls, name: str) -> ResourceHandleT|None:
        async with cls.class_lock:
            resource_handle = cls.all_instances.pop(name, None)
            if resource_handle:
                resource_handle.__unregister()
                return resource_handle

    def __str__(self) -> str:
        return f"ResourceHandle {self.name}"

    def __register(self) -> None:
        """
        Add ResourceHandle to register of bound or unbound instances.
        This method must be called with the ResourceHandle.lock held.
        """
        # Ensure deleting resource handles are not cached
        if self.is_deleting:
            self.__unregister()
            return
        self.all_instances[self.name] = self
        if self.is_bound:
            self.bound_instances[(
                self.resource_claim_namespace,
                self.resource_claim_name
            )] = self
            self.unbound_instances.pop(self.name, None)
        else:
            self.unbound_instances[self.name] = self

    def __unregister(self) -> None:
        self.all_instances.pop(self.name, None)
        self.unbound_instances.pop(self.name, None)
        if self.is_bound:
            self.bound_instances.pop(
                (self.resource_claim_namespace, self.resource_claim_name),
                None,
            )

    @property
    def guid(self) -> str:
        name = self.name
        generate_name = self.meta.get('generateName')
        if generate_name and name.startswith(generate_name):
            return name[len(generate_name):]
        elif name.startswith('guid-'):
            return name[5:]
        return name[-5:]

    @property
    def has_lifespan_end(self) -> bool:
        'end' in self.spec.get('lifespan', {})

    @property
    def has_resource_provider(self) -> bool:
        """Return whether this ResourceHandle is managed by a ResourceProvider."""
        return 'provider' in self.spec

    @property
    def ignore(self) -> bool:
        """Return whether this ResourceHandle should be ignored"""
        return Poolboy.ignore_label in self.labels

    @property
    def is_bound(self) -> bool:
        return 'resourceClaim' in self.spec

    @property
    def is_deleting(self) -> bool:
        return True if self.deletion_timestamp else False

    @property
    def is_from_resource_pool(self) -> bool:
        return 'resourcePool' in self.spec

    @property
    def is_healthy(self) -> bool|None:
        """Return overall health of resources.
        - False if any resource has healthy False.
        - None if any non-waiting resource lacks a value for healthy.
        - True if all non-waiting resources are healthy."""
        ret = True
        for resource in self.status_resources:
            if resource.get('healthy') is False:
                return False
            if(
                resource.get('waitingFor') is not None and
                resource.get('healthy') is None
            ):
                ret = None
        return ret

    @property
    def is_ready(self) -> bool|None:
        """Return overall readiness of resources.
        - False if any resource has ready False.
        - None if any non-waiting resource lacks a value for ready.
        - True if all non-waiting resources are ready."""
        ret = True
        for resource in self.status_resources:
            if resource.get('ready') is False:
                return False
            if(
                resource.get('waitingFor') is not None and
                resource.get('ready') is None
            ):
                ret = None
        return ret

    @property
    def is_past_lifespan_end(self) -> bool:
        dt = self.lifespan_end_datetime
        if not dt:
            return False
        return dt < datetime.now(timezone.utc)

    @property
    def is_ready(self) -> bool|None:
        return self.status.get('ready')

    @property
    def lifespan_end_datetime(self) -> Any:
        timestamp = self.lifespan_end_timestamp
        if timestamp:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z')

    @property
    def lifespan_end_timestamp(self) -> str|None:
        lifespan = self.spec.get('lifespan')
        if lifespan:
            return lifespan.get('end')

    @property
    def parameter_values(self) -> Mapping:
        return self.spec.get('provider', {}).get('parameterValues', {})

    @property
    def resource_claim_description(self) -> str|None:
        if not 'resourceClaim' not in self.spec:
            return None
        return f"ResourceClaim {self.resource_claim_name} in {self.resource_claim_namespace}"

    @property
    def resource_claim_name(self) -> str|None:
        return self.spec.get('resourceClaim', {}).get('name')

    @property
    def resource_claim_namespace(self) -> str|None:
        return self.spec.get('resourceClaim', {}).get('namespace')

    @property
    def resource_handler_idx(self) -> int:
        """Label value used to select which resource handler pod should manage this ResourceHandle."""
        return int(UUID(self.uid)) % Poolboy.resource_handler_count

    @property
    def resource_pool_name(self) -> str|None:
        if 'resourcePool' in self.spec:
            return self.spec['resourcePool']['name']

    @property
    def resource_pool_namespace(self) -> str|None:
        if 'resourcePool' in self.spec:
            return self.spec['resourcePool'].get('namespace', Poolboy.namespace)

    @property
    def resource_provider_name(self) -> str|None:
        return self.spec.get('provider', {}).get('name')

    @property
    def resources(self) -> List[Mapping]:
        """Resources as listed in spec."""
        return self.spec.get('resources', [])

    @property
    def status_resources(self) -> List[Mapping]:
        """Resources as listed in status."""
        return self.status.get('resources', [])

    @property
    def vars(self) -> Mapping:
        return self.spec.get('vars', {})

    @property
    def timedelta_to_lifespan_end(self) -> Any:
        dt = self.lifespan_end_datetime
        if dt:
            return dt - datetime.now(timezone.utc)

    def __lifespan_value(self, name, resource_claim):
        value = self.spec.get('lifespan', {}).get(name)
        if not value:
            return

        value = recursive_process_template_strings(
            template = value,
            variables = {
                "resource_claim": resource_claim,
                "resource_handle": self,
            },
            template_variables = self.vars,
        )

        return value

    def __lifespan_value_as_timedelta(self,
        name: str,
        resource_claim: ResourceClaimT,
    ) -> timedelta|None:
        value = self.__lifespan_value(name, resource_claim)
        if not value:
            return None
        seconds = pytimeparse.parse(value)
        if seconds is None:
            raise kopf.TemporaryError(f"Failed to parse {name} time interval: {value}", delay=60)
        return timedelta(seconds=seconds)

    async def __manage_init_status_resources(self) -> None:
        """Initialize resources in status from spec."""
        patch = []
        if not self.status:
            patch.append({
                "op": "add",
                "path": "/status",
                "value": {},
            })
        status_resources = self.status.get('resources', [])
        if 'resources' not in self.status:
            patch.append({
                "op": "add",
                "path": "/status/resources",
                "value": [],
            })
        for idx, resource in enumerate(self.resources):
            if idx < len(status_resources):
                status_resource = status_resources[idx]
            else:
                status_resource = {}
                status_resources.append(status_resource)
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}",
                    "value": {},
                })

            if 'name' in resource and resource['name'] != status_resource.get('name'):
                status_resource['name'] = resource['name']
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}/name",
                    "value": resource['name'],
                })

        if patch:
            await self.json_patch_status(patch)

    async def __manage_check_delete(self,
        logger: kopf.ObjectLogger,
        resource_claim: ResourceClaimT
    ) -> bool:
        """Delete this ResourceHandle if it meets conditions which trigger delete.
        - Is past lifespan end.
        - Is bound to resource claim that has been deleted.
        """
        if self.is_past_lifespan_end:
            logger.info(f"Deleting {self} at end of lifespan ({self.lifespan_end_timestamp})")
            await self.delete()
            return True
        if self.is_bound and not resource_claim:
            logger.info(f"Propagating deletion of {self.resource_claim_description} to {self}")
            await self.delete()
            return True

    async def __manage_update_spec_resources(self,
        logger: kopf.ObjectLogger,
        resource_claim: ResourceClaimT|None,
        resource_provider: ResourceProviderT|None,
    ):
        """Update this ResourecHandle's spec.resources by applying parameter values from ResourceProvider."""
        if not resource_provider:
            return

        resources = await resource_provider.get_resources(
            resource_claim = resource_claim,
            resource_handle = self,
        )

        if not 'resources' in self.spec:
            await self.json_patch([{
                "op": "add",
                "path": "/spec/resources",
                "value": resources,
            }])
            return

        patch = []
        for idx, resource in enumerate(resources):
            if idx < len(self.spec['resources']):
                current_provider = self.spec['resources'][idx]['provider']['name']
                updated_provider = resource['provider']['name']
                if current_provider != updated_provider:
                    logger.warning(
                        f"Refusing update resources in {self} as it would change "
                        f"ResourceProvider from {current_provider} to {updated_provider}"
                    )
                current_template = self.spec['resources'][idx].get('template')
                updated_template = resource.get('template')
                if current_template != updated_template:
                    patch.append({
                        "op": "add",
                        "path": f"/spec/resources/{idx}/template",
                        "value": updated_template,
                    })
            else:
                patch.append({
                    "op": "add",
                    "path": f"/spec/resources/{idx}",
                    "value": resource
                })

        if patch:
            await self.json_patch(patch)
            logger.info(f"Updated resources for {self} from {resource_provider}")

    def get_lifespan_default(self, resource_claim=None):
        return self.__lifespan_value('default', resource_claim=resource_claim)

    def get_lifespan_default_timedelta(self, resource_claim=None):
        return self.__lifespan_value_as_timedelta('default', resource_claim=resource_claim)

    def get_lifespan_maximum(self, resource_claim=None):
        return self.__lifespan_value('maximum', resource_claim=resource_claim)

    def get_lifespan_maximum_timedelta(self, resource_claim=None):
        return self.__lifespan_value_as_timedelta('maximum', resource_claim=resource_claim)

    def get_lifespan_relative_maximum(self, resource_claim=None):
        return self.__lifespan_value('relativeMaximum', resource_claim=resource_claim)

    def get_lifespan_relative_maximum_timedelta(self, resource_claim=None):
        return self.__lifespan_value_as_timedelta('relativeMaximum', resource_claim=resource_claim)

    def get_lifespan_end_maximum_datetime(self, resource_claim=None):
        lifespan_start_datetime = resource_claim.lifespan_start_datetime if resource_claim else self.creation_datetime

        maximum_timedelta = self.get_lifespan_maximum_timedelta(resource_claim=resource_claim)
        if maximum_timedelta:
            if resource_claim.lifespan_first_ready_timestamp:
                maximum_end = resource_claim.lifespan_first_ready_datetime + maximum_timedelta
            else:
                maximum_end = lifespan_start_datetime + maximum_timedelta
        else:
            maximum_end = None

        relative_maximum_timedelta = self.get_lifespan_relative_maximum_timedelta(resource_claim=resource_claim)
        if relative_maximum_timedelta:
            relative_maximum_end = datetime.now(timezone.utc) + relative_maximum_timedelta
        else:
            relative_maximum_end = None

        if relative_maximum_end \
        and (not maximum_end or relative_maximum_end < maximum_end):
            return relative_maximum_end
        return maximum_end

    def set_resource_healthy(self, resource_index: int, value: bool|None) -> None:
        if value is None:
            self.status['resources'][resource_index].pop('healthy', None)
        else:
            self.status['resources'][resource_index]['healthy'] = value

    def set_resource_ready(self, resource_index: int, value: bool|None) -> None:
        if value is None:
            self.status['resources'][resource_index].pop('ready', None)
        else:
            self.status['resources'][resource_index]['ready'] = value

    def set_resource_state(self, resource_index: int, value: Mapping|None) -> None:
        if value is None:
            self.status['resources'][resource_index].pop('state', None)
        else:
            self.status['resources'][resource_index]['state'] = value

    async def add_resource_handler_label(self):
        if self.labels.get(Poolboy.resource_handler_idx_label) != str(self.resource_handler_idx):
            await self.merge_patch({
                "metadata": {
                    "labels": {
                        Poolboy.resource_handler_idx_label: str(self.resource_handler_idx)
                    }
                }
            })

    async def get_resource_claim(self, not_found_okay: bool) -> ResourceClaimT|None:
        if not self.is_bound:
            return None
        try:
            return await resourceclaim.ResourceClaim.get(
                name = self.resource_claim_name,
                namespace = self.resource_claim_namespace,
                use_cache = Poolboy.operator_mode_all_in_one,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404 and not_found_okay:
                return None
            raise

    async def get_resource_pool(self) -> ResourcePoolT|None:
        if not self.is_from_resource_pool:
            return None
        return await resourcepool.ResourcePool.get(self.resource_pool_name)

    async def get_resource_provider(self) -> ResourceProviderT|None:
        """Return ResourceProvider configured to manage ResourceHandle."""
        if self.resource_provider_name:
            return await resourceprovider.ResourceProvider.get(self.resource_provider_name)

    async def get_resource_providers(self) -> List[ResourceProviderT]:
        """Return list of ResourceProviders for all managed resources."""
        resource_providers = []
        for resource in self.spec.get('resources', []):
            resource_providers.append(
                await resourceprovider.ResourceProvider.get(resource['provider']['name'])
            )
        return resource_providers

    async def get_resource_states(self) -> List[Mapping]:
        """Return list of states fom resources referenced by ResourceHandle."""
        resource_states = []
        for idx in range(len(self.spec['resources'])):
            reference = None
            if idx < len(self.status.get('resources', [])):
                reference = self.status['resources'][idx].get('reference')
            if not reference:
                resource_states.append(None)
                continue
            resource_states.append(
                await resourcewatch.ResourceWatch.get_resource_from_any(
                    api_version=reference['apiVersion'],
                    kind=reference['kind'],
                    name=reference['name'],
                    namespace=reference.get('namespace'),
                    not_found_okay=True,
                    use_cache=Poolboy.operator_mode_all_in_one,
                )
            )
        return resource_states

    async def handle_delete(self, logger: kopf.ObjectLogger) -> None:
        for resource in self.status.get('resources', []):
            reference = resource.get('reference')
            if reference:
                try:
                    resource_description = f"{reference['apiVersion']} {reference['kind']} " + (
                        f"{reference['name']} in {reference['namespace']}"
                        if 'namespace' in reference else reference['name']
                    )
                    logger.info(f"Propagating delete of {self} to {resource_description}")
                    # Annotate managed resource to indicate resource handle deletion.
                    await poolboy_k8s.patch_object(
                        api_version = reference['apiVersion'],
                        kind = reference['kind'],
                        name = reference['name'],
                        namespace = reference.get('namespace'),
                        patch = [{
                            "op": "add",
                            "path": f"/metadata/annotations/{Poolboy.resource_handle_deleted_annotation.replace('/', '~1')}",
                            "value": datetime.now(timezone.utc).strftime('%FT%TZ'),
                        }],
                    )
                    # Delete managed resource
                    await poolboy_k8s.delete_object(
                        api_version = reference['apiVersion'],
                        kind = reference['kind'],
                        name = reference['name'],
                        namespace = reference.get('namespace'),
                    )
                except kubernetes_asyncio.client.exceptions.ApiException as e:
                    if e.status != 404:
                        raise

        resource_claim = await self.get_resource_claim(not_found_okay=True)
        if resource_claim and not resource_claim.is_detached:
            await resource_claim.delete()
            logger.info(f"Propagated delete of {self} to ResourceClaim {resource_claim}")

        if self.is_from_resource_pool:
            resource_pool = await resourcepool.ResourcePool.get(self.resource_pool_name)
            if resource_pool:
                await resource_pool.manage(logger=logger)

        self.__unregister()

    async def manage(self, logger: kopf.ObjectLogger) -> None:
        """Main function for periodic management of ResourceHandle."""
        if self.ignore:
            return
        async with self.lock:
            # Get ResourceClaim bound to this ResourceHandle if there is one.
            resource_claim = await self.get_resource_claim(not_found_okay=True)
            # Delete this ResourceHandle if it meets delete trigger conditions.
            if await self.__manage_check_delete(logger=logger, resource_claim=resource_claim):
                return

            # Get top-level ResourceProvider managing this ResourceHandle
            resource_provider = await self.get_resource_provider()
            await self.__manage_update_spec_resources(
                logger=logger,
                resource_claim=resource_claim,
                resource_provider=resource_provider,
            )

            # Initialize status.resources
            await self.__manage_init_status_resources()

            # Get resource providers for managed resources
            resource_providers = await self.get_resource_providers()
            resource_states = await self.get_resource_states()
            resources_to_create = []
            patch = []

            # Loop through management for each managed resource
            for resource_index in range(len(self.resources)):
                status_resource = self.status_resources[resource_index]
                resource_state = resource_states[resource_index]
                resource_provider = resource_providers[resource_index]

                if resource_provider.resource_requires_claim and not resource_claim:
                    if 'ResourceClaim' != status_resource.get('waitingFor'):
                        patch.append({
                            "op": "add",
                            "path": f"/status/resources/{resource_index}/waitingFor",
                            "value": "ResourceClaim",
                        })
                    continue

                vars_ = deepcopy(self.vars)
                wait_for_linked_provider = False
                for linked_provider in resource_provider.linked_resource_providers:
                    linked_resource_provider = None
                    linked_resource_state = None
                    for pn, pv in enumerate(resource_providers):
                        if (
                            pv.name == linked_provider.name and
                            self.resources[pn].get('name', pv.name) == linked_provider.resource_name
                        ):
                            linked_resource_provider = pv
                            linked_resource_state = resource_states[pn]
                            break
                    else:
                        logger.debug(
                            f"{self} uses {resource_provider} which has "
                            f"linked ResourceProvider {resource_provider.name} but no resource in this "
                            f"ResourceHandle uses this provider."
                        )
                        continue

                    if not linked_provider.check_wait_for(
                        linked_resource_provider = linked_resource_provider,
                        linked_resource_state = linked_resource_state,
                        resource_claim = resource_claim,
                        resource_handle = self,
                        resource_provider = resource_provider,
                        resource_state = resource_state,
                    ):
                        wait_for_linked_provider = True
                        break

                    if linked_resource_state:
                        for template_var in linked_provider.template_vars:
                            vars_[template_var.name] = jsonpointer.resolve_pointer(
                                linked_resource_state, template_var.value_from,
                                default = jinja2.ChainableUndefined()
                            )

                if wait_for_linked_provider:
                    if 'Linked ResourceProvider' != status_resource.get('waitingFor'):
                        patch.append({
                            "op": "add",
                            "path": f"/status/resources/{resource_index}/waitingFor",
                            "value": "Linked ResourceProvider",
                        })
                    continue

                resource_definition = await resource_provider.resource_definition_from_template(
                    logger = logger,
                    resource_claim = resource_claim,
                    resource_handle = self,
                    resource_index = resource_index,
                    resource_states = resource_states,
                    vars_ = vars_,
                )
                if not resource_definition:
                    if 'Resource Definition' != status_resource.get('waitingFor'):
                        patch.append({
                            "op": "add",
                            "path": f"/status/resources/{resource_index}/waitingFor",
                            "value": "Resource Definition",
                        })
                    continue

                resource_api_version = resource_definition['apiVersion']
                resource_kind = resource_definition['kind']
                resource_name = resource_definition['metadata']['name']
                resource_namespace = resource_definition['metadata'].get('namespace', None)

                reference = {
                    'apiVersion': resource_api_version,
                    'kind': resource_kind,
                    'name': resource_name
                }
                if resource_namespace:
                    reference['namespace'] = resource_namespace

                if 'reference' not in status_resource:
                    # Add reference to status resources
                    status_resource['reference'] = reference
                    patch.append({
                        "op": "add",
                        "path": f"/status/resources/{resource_index}/reference",
                        "value": reference,
                    })
                    # Remove waitingFor from status if present as we are preceeding to resource creation
                    if 'waitingFor' in status_resource:
                        patch.append({
                            "op": "remove",
                            "path": f"/status/resources/{resource_index}/waitingFor",
                        })
                elif resource_api_version != status_resource['reference']['apiVersion']:
                    raise kopf.TemporaryError(
                        f"ResourceHandle {self.name} would change from apiVersion "
                        f"{status_resource['reference']['apiVersion']} to {resource_api_version}!",
                        delay=600
                    )
                elif resource_kind != status_resource['reference']['kind']:
                    raise kopf.TemporaryError(
                        f"ResourceHandle {self.name} would change from kind "
                        f"{status_resource['reference']['kind']} to {resource_kind}!",
                        delay=600
                    )
                else:
                    # Maintain name and namespace
                    if resource_name != status_resource['reference']['name']:
                        resource_name = status_resource['reference']['name']
                        resource_definition['metadata']['name'] = resource_name
                    if resource_namespace != status_resource['reference'].get('namespace'):
                        resource_namespace = status_resource['reference']['namespace']
                        resource_definition['metadata']['namespace'] = resource_namespace

                resource_description = f"{resource_api_version} {resource_kind} {resource_name}"
                if resource_namespace:
                    resource_description += f" in {resource_namespace}"

                # Ensure there is a ResourceWatch for this resource.
                await resourcewatch.ResourceWatch.create_as_needed(
                    api_version = resource_api_version,
                    kind = resource_kind,
                    namespace = resource_namespace,
                )

                if resource_state:
                    updated_state = await resource_provider.update_resource(
                        logger = logger,
                        resource_definition = resource_definition,
                        resource_handle = self,
                        resource_state = resource_state,
                    )
                    if updated_state:
                        resource_states[resource_index] = updated_state
                        logger.info(f"Updated {resource_description} for ResourceHandle {self.name}")
                else:
                    resources_to_create.append((resource_index, resource_definition))

            if patch:
                try:
                    await self.json_patch_status(patch)
                except kubernetes_asyncio.client.exceptions.ApiException as exception:
                    if exception.status == 422:
                        logger.error(f"Failed to apply {patch}")
                    raise

            for resource_index, resource_definition in resources_to_create:
                resource_api_version = resource_definition['apiVersion']
                resource_kind = resource_definition['kind']
                resource_name = resource_definition['metadata']['name']
                resource_namespace = resource_definition['metadata'].get('namespace', None)
                resource_description = f"{resource_api_version} {resource_kind} {resource_name}"
                if resource_namespace:
                    resource_description += f" in {resource_namespace}"
                try:
                    created_resource = await poolboy_k8s.create_object(resource_definition)
                    if created_resource:
                        resource_states[resource_index] = created_resource
                        logger.info(f"Created {resource_description} for {self}")
                except kubernetes_asyncio.client.exceptions.ApiException as exception:
                    if exception.status != 409:
                        raise

            if resource_claim:
                await resource_claim.update_status_from_handle(
                    logger=logger,
                    resource_handle=self,
                    resource_states=resource_states,
                )

    async def refetch(self) -> ResourceHandleT|None:
        try:
            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                Poolboy.operator_domain, Poolboy.operator_version, Poolboy.namespace, 'resourcehandles', self.name
            )
            self.refresh_from_definition(definition)
            return self
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404:
                await self.unregister(name=self.name)
                return None
            raise

    async def update_status(self,
        logger: kopf.ObjectLogger,
        resource_states: List[Mapping|None],
        resource_claim: ResourceClaimT|None=None,
    ) -> None:
        """Update status from resources state."""
        status = self.status

        # Create consolidated information about resources
        resources = deepcopy(self.resources)
        for idx, state in enumerate(resource_states):
            resources[idx]['state'] = state

        patch = []
        have_healthy_resource = False
        all_resources_healthy = True
        have_ready_resource = False
        all_resources_ready = True

        if not status:
            patch.append({
                "op": "add",
                "path": "/status",
                "value": {},
            })
        status_resources = status.get('resources')
        if status_resources is None:
            status_resources = []
            patch.append({
                "op": "add",
                "path": "/status/resources",
                "value": [],
            })

        for idx, resource in enumerate(resources):
            if idx < len(status_resources):
                status_resource = status_resources[idx]
            else:
                status_resource = {}
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}",
                    "value": {},
                })

            resource_healthy = None
            resource_ready = False

            state = resource.get('state')
            if state:
                resource_provider = await resourceprovider.ResourceProvider.get(resource['provider']['name'])
                resource_healthy = resource_provider.check_health(
                    logger=logger,
                    resource_handle=self,
                    resource_state=state,
                )
                if resource_healthy is False:
                    resource_ready = False
                else:
                    resource_ready = resource_provider.check_readiness(
                        logger=logger,
                        resource_handle=self,
                        resource_state=state,
                    )

            if resource_healthy is True:
                have_healthy_resource = True
            elif resource_healthy is False:
                all_resources_healthy = False
            elif all_resources_healthy:
                all_resources_healthy = None

            if resource_ready is True:
                have_ready_resource = True
            elif resource_ready is False:
                all_resources_ready = False
            elif all_resources_ready is True:
                all_resources_ready = None

            if resource_healthy is None and 'healthy' in status_resource:
                patch.append({
                    "op": "remove",
                    "path": f"/status/resources/{idx}/healthy",
                })
            elif resource_healthy != status_resource.get('healthy'):
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}/healthy",
                    "value": resource_healthy,
                })

            if resource_ready is None and 'ready' in status_resource:
                patch.append({
                    "op": "remove",
                    "path": f"/status/resources/{idx}/ready",
                })
            elif resource_ready != status_resource.get('ready'):
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}/ready",
                    "value": resource_ready,
                })

        if all_resources_healthy and not have_healthy_resource:
            all_resources_healthy = None
        if all_resources_ready and not have_ready_resource:
            all_resources_ready = None

        if all_resources_healthy is None:
            if 'healthy' in status:
                patch.append({
                    "op": "remove",
                    "path": "/status/healthy",
                })
        elif all_resources_healthy != status.get('healthy'):
            patch.append({
                "op": "add",
                "path": "/status/healthy",
                "value": all_resources_healthy,
            })

        if all_resources_ready is None:
            if 'ready' in status:
                patch.append({
                    "op": "remove",
                    "path": "/status/ready",
                })
        elif all_resources_ready != status.get('ready'):
            patch.append({
                "op": "add",
                "path": "/status/ready",
                "value": all_resources_ready,
            })

        if self.has_resource_provider:
            resource_provider = None
            try:
                resource_provider = await self.get_resource_provider()
                if resource_provider.status_summary_template:
                    status_summary = resource_provider.make_status_summary(
                        resource_handle=self,
                        resources=resources,
                    )
                    if status_summary != status.get('summary'):
                        patch.append({
                            "op": "add",
                            "path": "/status/summary",
                            "value": status_summary,
                        })
            except kubernetes_asyncio.client.exceptions.ApiException:
                logger.exception(
                    f"Failed to get ResourceProvider {self.resource_provider_name} for {self}"
                )
            except Exception:
                logger.exception(f"Failed to generate status summary for {self}")
        if patch:
            await self.json_patch_status(patch)

        if resource_claim:
            await resource_claim.update_status_from_handle(
                logger=logger,
                resource_handle=self,
                resource_states=resource_states,
            )
