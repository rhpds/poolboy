import asyncio
import jinja2
import jsonpointer
import kopf
import kubernetes_asyncio
import logging
import pytimeparse

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Optional, TypeVar, Union

import poolboy_k8s
import resourceclaim
import resourcepool
import resourceprovider
import resourcewatcher

from kopfobject import KopfObject
from poolboy import Poolboy
from poolboy_templating import recursive_process_template_strings, seconds_to_interval, timedelta_to_str

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
        if self.resource_handle.is_healthy and cmp.resource_handle.is_healthy == None:
            return True
        if self.resource_handle.is_healthy == None and cmp.resource_handle.is_healthy:
            return False

        # Prefer ready resources to unready or unknown readiness state
        if self.resource_handle.is_ready and not cmp.resource_handle.is_ready:
            return True
        if not self.resource_handle.is_ready and cmp.resource_handle.is_ready:
            return False

        # Prefer unknown readiness state to known unready state
        if self.resource_handle.is_ready == None and cmp.resource_handle.is_ready == False:
            return True
        if not self.resource_handle.is_ready == False and cmp.resource_handle.is_ready == None:
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
    ) -> Optional[ResourceHandleT]:
        async with cls.class_lock:
            # Check if there is already an assigned claim
            resource_handle = cls.bound_instances.get((resource_claim.namespace, resource_claim.name))
            if resource_handle:
                if await resource_handle.refetch():
                    logger.warning(f"Rebinding {resource_handle} to {resource_claim}")
                    return resource_handle
                else:
                    logger.warning(f"Deleted {resource_handle} was still in memory cache")

            claim_status_resources = resource_claim.status_resources

            # Loop through unbound instances to find best match
            matches = []
            for resource_handle in cls.unbound_instances.values():
                # Skip unhealthy
                if resource_handle.is_healthy == False:
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
                    if diff_patch == None:
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
                    f"{Poolboy.operator_domain}/resource-claim-name": resource_claim.name,
                    f"{Poolboy.operator_domain}/resource-claim-namespace": resource_claim.namespace,
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
        resource_handle = await cls.register_definition(definition=definition)
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
                    f"{Poolboy.operator_domain}/resource-pool-name": resource_pool.name,
                    f"{Poolboy.operator_domain}/resource-pool-namespace": resource_pool.namespace,
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
        resource_handle = await cls.register_definition(definition=definition)
        logger.info(f"Created ResourceHandle {resource_handle.name} for ResourcePool {resource_pool.name}")
        return resource_handle

    @classmethod
    async def delete_unbound_handles_for_pool(
        cls,
        logger: kopf.ObjectLogger,
        resource_pool: ResourcePoolT,
    ) -> List[ResourceHandleT]:
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

    @classmethod
    async def get(cls, name: str, ignore_deleting=True) -> Optional[ResourceHandleT]:
        async with cls.class_lock:
            resource_handle = cls.all_instances.get(name)
            if resource_handle:
                return resource_handle
            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                Poolboy.operator_domain, Poolboy.operator_version, Poolboy.namespace, 'resourcehandles', name
            )
            if ignore_deleting and 'deletionTimestamp' in definition['metadata']:
                return None
            return cls.__register_definition(definition=definition)

    @classmethod
    def get_from_cache(cls, name: str) -> Optional[ResourceHandleT]:
        return cls.all_instances.get(name)

    @classmethod
    async def get_unbound_handles_for_pool(
        cls,
        resource_pool: ResourcePoolT,
        logger: kopf.ObjectLogger,
    ) -> List[ResourceHandleT]:
        async with cls.class_lock:
            resource_handles = []
            for resource_handle in ResourceHandle.unbound_instances.values():
                if resource_handle.resource_pool_name == resource_pool.name \
                and resource_handle.resource_pool_namespace == resource_pool.namespace:
                    resource_handles.append(resource_handle)
            return resource_handles

    @classmethod
    async def preload(cls, logger: kopf.ObjectLogger) -> None:
        async with cls.class_lock:
            _continue = None
            while True:
                resource_handle_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
                    Poolboy.operator_domain, Poolboy.operator_version, Poolboy.namespace, 'resourcehandles',
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
    async def unregister(cls, name: str) -> Optional[ResourceHandleT]:
        async with cls.class_lock:
            resource_handle = cls.all_instances.pop(name, None)
            if resource_handle:
                resource_handle.__unregister()
                return resource_handle

    def __init__(self,
        annotations: Union[kopf.Annotations, Mapping],
        labels: Union[kopf.Labels, Mapping],
        meta: Union[kopf.Meta, Mapping],
        name: str,
        namespace: str,
        spec: Union[kopf.Spec, Mapping],
        status: Union[kopf.Status, Mapping],
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
        else:
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
    def is_healthy(self) -> Optional[bool]:
        return self.status.get('healthy')

    @property
    def is_past_lifespan_end(self) -> bool:
        dt = self.lifespan_end_datetime
        if not dt:
            return False
        return dt < datetime.now(timezone.utc)

    @property
    def is_ready(self) -> Optional[bool]:
        return self.status.get('ready')

    @property
    def lifespan_end_datetime(self) -> Any:
        timestamp = self.lifespan_end_timestamp
        if timestamp:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S%z')

    @property
    def lifespan_end_timestamp(self) -> Optional[str]:
        lifespan = self.spec.get('lifespan')
        if lifespan:
            return lifespan.get('end')

    @property
    def parameter_values(self) -> Mapping:
        return self.spec.get('provider', {}).get('parameterValues', {})

    @property
    def resource_claim_description(self) -> Optional[str]:
        if not 'resourceClaim' not in self.spec:
            return None
        return f"ResourceClaim {self.resource_claim_name} in {self.resource_claim_namespace}"

    @property
    def resource_claim_name(self) -> Optional[str]:
        return self.spec.get('resourceClaim', {}).get('name')

    @property
    def resource_claim_namespace(self) -> Optional[str]:
        return self.spec.get('resourceClaim', {}).get('namespace')

    @property
    def resource_pool_name(self) -> Optional[str]:
        if 'resourcePool' in self.spec:
            return self.spec['resourcePool']['name']

    @property
    def resource_pool_namespace(self) -> Optional[str]:
        if 'resourcePool' in self.spec:
            return self.spec['resourcePool'].get('namespace', Poolboy.namespace)

    @property
    def resource_provider_name(self) -> Optional[str]:
        return self.spec.get('provider', {}).get('name')

    @property
    def resources(self) -> List[Mapping]:
        return self.spec.get('resources', [])

    @property
    def status_resources(self) -> List[Mapping]:
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

    def __lifespan_value_as_timedelta(self, name, resource_claim):
        value = self.__lifespan_value(name, resource_claim)
        if not value:
            return

        seconds = pytimeparse.parse(value)
        if seconds == None:
            raise kopf.TemporaryError(f"Failed to parse {name} time interval: {value}", delay=60)

        return timedelta(seconds=seconds)

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
        else:
            return maximum_end

    async def get_resource_claim(self) -> Optional[ResourceClaimT]:
        if not self.is_bound:
            return None
        return await resourceclaim.ResourceClaim.get(
            name = self.resource_claim_name,
            namespace = self.resource_claim_namespace,
        )

    async def get_resource_pool(self) -> Optional[ResourcePoolT]:
        if not self.is_from_resource_pool:
            return None
        return await resourcepool.ResourcePool.get(self.resource_pool_name)

    async def get_resource_provider(self) -> ResourceProviderT:
        """Return ResourceProvider configured to manage ResourceHandle."""
        return await resourceprovider.ResourceProvider.get(self.resource_provider_name)

    async def get_resource_providers(self) -> List[ResourceProviderT]:
        resource_providers = []
        for resource in self.spec.get('resources', []):
            resource_providers.append(
                await resourceprovider.ResourceProvider.get(resource['provider']['name'])
            )
        return resource_providers

    async def get_resource_states(self, logger: kopf.ObjectLogger) -> List[Mapping]:
        resource_states = []
        for resource_index, resource in enumerate(self.resources):
            if resource_index >= len(self.status_resources):
                resource_states.append(None)
                continue
            reference = self.status_resources[resource_index].get('reference')
            if not reference:
                resource_states.append(None)
                continue
            api_version = reference['apiVersion']
            kind = reference['kind']
            name = reference['name']
            namespace = reference.get('namespace')
            resource = await resourcewatcher.ResourceWatcher.get_resource(
                api_version=api_version, kind=kind, name=name, namespace=namespace,
            )
            resource_states.append(resource)
        return resource_states

    async def handle_delete(self, logger: kopf.ObjectLogger) -> None:
        for resource in self.spec.get('resources', []):
            reference = resource.get('reference')
            if reference:
                try:
                    resource_description = f"{reference['apiVersion']} {reference['kind']} " + (
                        f"{reference['name']} in {reference['namespace']}"
                        if 'namespace' in reference else reference['name']
                    )
                    logger.info(f"Propagating delete of {self} to {resource_description}")
                    await poolboy_k8s.delete_object(
                        api_version = reference['apiVersion'],
                        kind = reference['kind'],
                        name = reference['name'],
                        namespace = reference.get('namespace'),
                    )
                except kubernetes_asyncio.client.exceptions.ApiException as e:
                    if e.status != 404:
                        raise

        if self.is_bound:
            try:
                resource_claim = await self.get_resource_claim()
                if not resource_claim.is_detached:
                    await resource_claim.delete()
                    logger.info(f"Propagated delete of ResourceHandle {self.name} to ResourceClaim {self.resource_claim_name} in {self.resource_claim_namespace}")
            except kubernetes_asyncio.client.exceptions.ApiException as e:
                if e.status != 404:
                    raise

        if self.is_from_resource_pool:
            resource_pool = await resourcepool.ResourcePool.get(self.resource_pool_name)
            if resource_pool:
                await resource_pool.manage(logger=logger)

        self.__unregister()

    async def handle_resource_event(self,
        logger: Union[logging.Logger, logging.LoggerAdapter],
    ) -> None:
        async with self.lock:
            await self.update_status(logger=logger)

    async def manage(self, logger: kopf.ObjectLogger) -> None:
        async with self.lock:
            resource_claim = None
            if self.is_bound:
                try:
                    resource_claim = await self.get_resource_claim()
                except kubernetes_asyncio.client.exceptions.ApiException as e:
                    if e.status == 404:
                        logger.info(
                            f"Propagating deletion of ResourceClaim "
                            f"{self.resource_claim_name} in {self.resource_claim_namespace} "
                            f"to ResourceHandle {self.name}"
                        )
                        await self.delete()
                    else:
                        raise

            if self.is_past_lifespan_end:
                logger.info(f"Deleting {self} at end of lifespan ({self.lifespan_end_timestamp})")
                await self.delete()
                return

            await self.update_resources(logger=logger, resource_claim=resource_claim)

            resource_providers = await self.get_resource_providers()
            resource_states = await self.get_resource_states(logger=logger)
            status_resources = self.status_resources
            patch = []
            status_patch = []
            resources_to_create = []

            if not self.status:
                status_patch.append({
                    "op": "add",
                    "path": "/status",
                    "value": {},
                })
            if 'resources' not in self.status:
                status_patch.append({
                    "op": "add",
                    "path": "/status/resources",
                    "value": [],
                })

            for resource_index, resource in enumerate(self.spec['resources']):
                resource_provider = resource_providers[resource_index]
                resource_state = resource_states[resource_index]

                if len(status_resources) <= resource_index:
                    status_resources.append({})
                    status_patch.append({
                        "op": "add",
                        "path": f"/status/resources/{resource_index}",
                        "value": {},
                    })
                status_resource = status_resources[resource_index]

                if 'name' in resource and resource['name'] != status_resource.get('name'):
                    status_resource['name'] = resource['name']
                    status_patch.append({
                        "op": "add",
                        "path": f"/status/resources/{resource_index}/name",
                        "value": resource['name'],
                    })

                if resource_provider.resource_requires_claim and not resource_claim:
                    if 'ResourceClaim' != status_resource.get('waitingFor'):
                        status_patch.append({
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
                        if pv.name == linked_provider.name:
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
                        status_patch.append({
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
                        status_patch.append({
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
                    status_patch.append({
                        "op": "add",
                        "path": f"/status/resources/{resource_index}/reference",
                        "value": reference,
                    })
                    # Retain reference in spec for compatibility
                    patch.append({
                        "op": "add",
                        "path": f"/spec/resources/{resource_index}/reference",
                        "value": reference,
                    })
                    # Remove waitingFor from status if present as we are preceeding to resource creation
                    if 'waitingFor' in status_resource:
                        status_patch.append({
                            "op": "remove",
                            "path": f"/status/resources/{resource_index}/waitingFor",
                        })
                    try:
                        # Get resource state as it would not have been fetched above.
                        resource_states[resource_index] = resource_state = await resourcewatcher.ResourceWatcher.get_resource(
                            api_version = resource_api_version,
                            kind = resource_kind,
                            name = resource_name,
                            namespace = resource_namespace,
                        )
                    except kubernetes_asyncio.client.exceptions.ApiException as e:
                        if e.status != 404:
                            raise
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

                await resourcewatcher.ResourceWatcher.start_resource_watch(
                    api_version = resource_api_version,
                    kind = resource_kind,
                    namespace = resource_namespace,
                )

                if resource_state:
                    changes = await resource_provider.update_resource(
                        logger = logger,
                        resource_definition = resource_definition,
                        resource_handle = self,
                        resource_state = resource_state,
                    )
                    if changes:
                        logger.info(f"Updated {resource_description} for ResourceHandle {self.name}")
                else:
                    resources_to_create.append(resource_definition)

            if patch:
                try:
                    await self.json_patch(patch)
                except kubernetes_asyncio.client.exceptions.ApiException as exception:
                    if exception.status == 422:
                        logger.error(f"Failed to apply {patch}")
                    raise

            if status_patch:
                try:
                    await self.json_patch_status(status_patch)
                except kubernetes_asyncio.client.exceptions.ApiException as exception:
                    if exception.status == 422:
                        logger.error(f"Failed to apply {status_patch}")
                    raise

            await self.update_status(logger=logger)

            if resource_claim:
                await resource_claim.update_status_from_handle(
                    logger=logger,
                    resource_handle=self,
                )

            for resource_definition in resources_to_create:
                changes = await poolboy_k8s.create_object(resource_definition)
                if changes:
                    logger.info(f"Created {resource_description} for ResourceHandle {self.name}")

    async def refetch(self) -> Optional[ResourceHandleT]:
        try:
            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                Poolboy.operator_domain, Poolboy.operator_version, Poolboy.namespace, 'resourcehandles', self.name
            )
            self.refresh_from_definition(definition)
            return self
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404:
                self.unregister(name=self.name)
                return None
            else:
                raise

    async def update_resources(self,
        logger: kopf.ObjectLogger,
        resource_claim: ResourceClaimT,
    ):
        # If no spec.provider then resources list is directly configured in the ResourceHandle
        if 'provider' not in self.spec:
            return

        try:
            provider = await resourceprovider.ResourceProvider.get(self.resource_provider_name)
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404:
                logger.warning(f"Missing ResourceProvider {self.resource_provider_name} to get resources for {self}")
                return
            else:
                raise

        resources = await provider.get_resources(
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
            logger.info(f"Updated resources for {self} from {provider}")

    async def update_status(self,
        logger: kopf.ObjectLogger,
    ) -> None:
        patch = []
        if not self.status:
            patch.append({
                "op": "add",
                "path": "/status",
                "value": {},
            })
        if not 'resources' in self.status:
            patch.append({
                "op": "add",
                "path": "/status/resources",
                "value": [],
            })

        resources = deepcopy(self.resources)
        resource_states = await self.get_resource_states(logger=logger)
        for idx, state in enumerate(resource_states):
            resources[idx]['state'] = state
            if len(self.status_resources) < idx:
                patch.append({
                    "op": "add",
                    "path": f"/status/resources/{idx}",
                    "value": {},
                })

        overall_ready = True
        overall_healthy = True

        for idx, resource in enumerate(resources):
            if resource['state']:
                resource_provider = await resourceprovider.ResourceProvider.get(resource['provider']['name'])
                resource_healthy = resource_provider.check_health(
                    logger = logger,
                    resource_handle = self,
                    resource_state = resource['state'],
                )
                if resource_healthy == False:
                    resource_ready = False
                else:
                    resource_ready = resource_provider.check_readiness(
                        logger = logger,
                        resource_handle = self,
                        resource_state = resource['state'],
                    )
            else:
                resource_healthy = None
                resource_ready = False

            # If the resource is not healthy then it is overall unhealthy.
            # If the resource health is unknown then he overall health is unknown unless it is unhealthy.
            if resource_healthy == False:
                overall_healthy = False
            elif resource_healthy == None:
                if overall_healthy:
                    overall_healthy = None

            if resource_ready == False:
                overall_ready = False
            elif resource_ready == None:
                if overall_ready:
                    overall_ready = None

            if len(self.status_resources) <= idx:
                if resource_healthy != None:
                    patch.append({
                        "op": "add",
                        "path": f"/status/resources/{idx}/healthy",
                        "value": resource_healthy,
                    })
                if resource_ready != None:
                    patch.append({
                        "op": "add",
                        "path": f"/status/resources/{idx}/ready",
                        "value": resource_ready,
                    })
            else:
                if resource_healthy == None:
                    if 'healthy' in self.status_resources[idx]:
                        patch.append({
                            "op": "remove",
                            "path": f"/status/resources/{idx}/healthy",
                        })
                elif resource_healthy != self.status_resources[idx].get('healthy'):
                    patch.append({
                        "op": "add",
                        "path": f"/status/resources/{idx}/healthy",
                        "value": resource_healthy,
                    })
                if resource_ready == None:
                    if 'ready' in self.status_resources[idx]:
                        patch.append({
                            "op": "remove",
                            "path": f"/status/resources/{idx}/ready",
                        })
                elif resource_ready != self.status_resources[idx].get('ready'):
                    patch.append({
                        "op": "add",
                        "path": f"/status/resources/{idx}/ready",
                        "value": resource_ready,
                    })

        if overall_healthy == None:
            if 'healthy' in self.status:
                patch.append({
                    "op": "remove",
                    "path": f"/status/healthy",
                })
        elif overall_healthy != self.status.get('healthy'):
            patch.append({
                "op": "add",
                "path": f"/status/healthy",
                "value": overall_healthy,
            })

        if overall_ready == None:
            if 'ready' in self.status:
                patch.append({
                    "op": "remove",
                    "path": f"/status/ready",
                })
        elif overall_ready != self.status.get('ready'):
            patch.append({
                "op": "add",
                "path": f"/status/ready",
                "value": overall_ready,
            })

        if self.has_resource_provider:
            resource_provider = await self.get_resource_provider()
            if resource_provider.status_summary_template:
                try:
                    status_summary = resource_provider.make_status_summary(
                        resource_handle=self,
                        resources=resources,
                    )
                    if status_summary != self.status.get('summary'):
                        patch.append({
                            "op": "add",
                            "path": "/status/summary",
                            "value": status_summary,
                        })
                except Exception:
                    logger.exception(f"Failed to generate status summary for {self}")

        if patch:
            await self.json_patch_status(patch)
