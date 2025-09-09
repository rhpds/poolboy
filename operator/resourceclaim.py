import asyncio

from copy import deepcopy
from datetime import datetime, timezone
from typing import List, Mapping, TypeVar

import kopf
import kubernetes_asyncio

from deep_merge import deep_merge
from kopfobject import KopfObject
from poolboy import Poolboy
from poolboy_templating import recursive_process_template_strings

import resourcehandle
import resourceprovider

ResourceClaimT = TypeVar('ResourceClaimT', bound='ResourceClaim')
ResourceHandleT = TypeVar('ResourceHandleT', bound='ResourceHandle')
ResourceProviderT = TypeVar('ResourceProviderT', bound='ResourceProvider')

def prune_k8s_resource(resource: Mapping) -> Mapping:
    ret = {
        key: value
        for key, value in resource.items()
        if key not in {"metadata", "status"}
    }
    ret["metadata"] = {
        key: value
        for key, value in resource['metadata'].items()
        if key not in {'annotations', 'generation', 'managedFields'}
    }
    if 'annotations' in resource['metadata']:
        filtered_annotations = {
            key: value
            for key, value in resource['metadata']['annotations'].items()
            if not key.startswith(f"{Poolboy.operator_domain}/")
        }
        if filtered_annotations:
            ret["metadata"]["annotations"] = filtered_annotations
    if 'status' in resource:
        ret["status"] = {
            key: value
            for key, value in resource['status'].items()
            if not key in {'diffBase'}
        }
    return ret

class ResourceClaim(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourceClaim"
    plural = "resourceclaims"

    instances = {}
    class_lock = asyncio.Lock()

    @classmethod
    def __register_definition(cls, definition: Mapping) -> ResourceClaimT:
        name = definition['metadata']['name']
        namespace = definition['metadata']['namespace']
        resource_claim = cls.instances.get((namespace, name))
        if resource_claim:
            resource_claim.refresh_from_definition(definition=definition)
        else:
            resource_claim = cls(
                annotations = definition['metadata'].get('annotations', {}),
                labels = definition['metadata'].get('labels', {}),
                meta = definition['metadata'],
                name = name,
                namespace = namespace,
                spec = definition['spec'],
                status = definition.get('status', {}),
                uid = definition['metadata']['uid'],
            )
            cls.instances[(namespace, name)] = resource_claim
        return resource_claim

    @classmethod
    async def get(cls, name: str, namespace: str, use_cache: bool=True) -> ResourceClaimT:
        async with cls.class_lock:
            if use_cache and (namespace, name) in cls.instances:
                return cls.instances[(namespace, name)]
            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                group=cls.api_group,
                name=name,
                namespace=namespace,
                plural=cls.plural,
                version=cls.api_version,
            )
            if use_cache:
                return cls.__register_definition(definition)
            return cls.from_definition(definition)

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
    ) -> ResourceClaimT:
        async with cls.class_lock:
            resource_claim = cls.instances.get((namespace, name))
            if resource_claim:
                resource_claim.refresh(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            else:
                resource_claim = cls(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    name = name,
                    namespace = namespace,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
                cls.instances[(namespace, name)] = resource_claim
            return resource_claim

    @classmethod
    async def register_definition(
        cls,
        definition: Mapping,
    ) -> ResourceClaimT:
        async with cls.class_lock:
            return cls.__register_definition(definition=definition)

    @classmethod
    async def unregister(cls, name: str, namespace: str) -> ResourceClaimT|None:
        async with cls.class_lock:
            return cls.instances.pop((namespace, name), None)

    @property
    def approval_state(self) -> str|None:
        """Return approval state of this ResourceClaim."""
        return self.status.get('approval', {}).get('state')

    @property
    def auto_delete_when(self) -> str|None:
        """Return condition which triggers automatic delete if defined."""
        return self.spec.get('autoDelete', {}).get('when')

    @property
    def auto_detach_when(self) -> str|None:
        """Return condition which triggers automatic detach if defined."""
        return self.spec.get('autoDetach', {}).get('when')

    @property
    def claim_is_initialized(self) -> bool:
        return f"{Poolboy.operator_domain}/resource-claim-init-timestamp" in self.annotations

    @property
    def has_resource_handle(self) -> bool:
        """Return whether this ResourceClaim is bound to  a ResourceHandle."""
        return self.status and 'resourceHandle' in self.status

    @property
    def has_resource_provider(self) -> bool:
        """Return whether this ResourceClaim is managed by a ResourceProvider."""
        return 'provider' in self.spec

    @property
    def has_spec_resources(self) -> bool:
        """Return whether this ResourceClaim is has an explicit list of Resources."""
        return 'resources' in self.spec

    @property
    def have_resource_providers(self) -> bool:
        """Return whether this ResourceClaim has ResourceProviders assigned for all resources."""
        if not self.status \
        or not 'resources' in self.status \
        or len(self.spec.get('resources', [])) > len(self.status.get('resources', [])):
            return False
        for resource in self.status.get('resources', []):
            if not 'provider' in resource:
                return False
        return True

    @property
    def ignore(self) -> bool:
        """Return whether this ResourceClaim should be ignored"""
        return Poolboy.ignore_label in self.labels

    @property
    def is_approved(self) -> bool:
        """Return whether this ResourceClaim has been approved.
        If claim is already has a resource hadle then it is considered approved."""
        return self.has_resource_handle or self.approval_state == 'approved'

    @property
    def is_detached(self) -> bool:
        """Return whether this ResourceClaim has been detached from its ResourceHandle."""
        if not self.status:
            return False
        return self.status.get('resourceHandle', {}).get('detached', False)

    @property
    def lifespan_end_datetime(self) -> datetime|None:
        """Return datetime object representing when this ResourceClaim will be automatically deleted.
        Return None if lifespan end is not set.
        """
        timestamp = self.lifespan_end_timestamp
        if timestamp:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")

    @property
    def lifespan_end_timestamp(self) -> str|None:
        """Return timestamp representing when this ResourceClaim will be automatically deleted.
        Return None if lifespan end is not set.
        """
        lifespan = self.status.get('lifespan')
        if lifespan:
            return lifespan.get('end')

    @property
    def lifespan_first_ready_datetime(self) -> datetime|None:
        timestamp = self.lifespan_first_ready_timestamp
        if timestamp:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")

    @property
    def lifespan_first_ready_timestamp(self) -> str|None:
        timestamp = self.status.get('lifespan', {}).get('firstReady')
        if timestamp:
            return timestamp

    @property
    def lifespan_maximum(self) -> str|None:
        lifespan = self.status.get('lifespan')
        if lifespan:
            return lifespan.get('maximum')

    @property
    def lifespan_relative_maximum(self) -> str|None:
        lifespan = self.status.get('lifespan')
        if lifespan:
            return lifespan.get('relativeMaximum')

    @property
    def lifespan_start_datetime(self) -> datetime|None:
        """Return datetime when lifespan of ResourceClaim started or is requested to start."""
        timestamp = self.lifespan_start_timestamp
        if timestamp:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")

    @property
    def lifespan_start_timestamp(self) -> str|None:
        """Return timestamp when lifespan of ResourceClaim started or is requested to start."""
        timestamp = self.status.get('lifespan', {}).get('start')
        if timestamp:
            return timestamp
        timestamp = self.spec.get('lifespan', {}).get('start')
        if timestamp:
            return timestamp
        return self.creation_timestamp

    @property
    def parameter_values(self) -> Mapping:
        return self.status.get('provider', {}).get('parameterValues', {})

    @property
    def requested_lifespan_end_datetime(self):
        timestamp = self.requested_lifespan_end_timestamp
        if timestamp:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")

    @property
    def requested_lifespan_end_timestamp(self) -> str|None:
        lifespan = self.spec.get('lifespan')
        if lifespan:
            return lifespan.get('end')

    @property
    def resource_handle_description(self) -> str:
        return f"ResourceHandle {self.resource_handle_name}"

    @property
    def resource_handle_name(self):
        if not self.status:
            return None
        return self.status.get('resourceHandle', {}).get('name')

    @property
    def resource_handle_namespace(self):
        if not self.status:
            return None
        return self.status.get('resourceHandle', {}).get('namespace')

    @property
    def resource_pool_name(self):
        if not self.annotations:
            return None
        return self.annotations.get(f"{Poolboy.operator_domain}/resource-pool-name")

    @property
    def resource_provider_name(self) -> str:
        """Return name of ResourceProvider which manages this ResourceClaim.
        Status provider configuration overrides spec to prevent changes to provider.
        """
        return self.resource_provider_name_from_status or self.resource_provider_name_from_spec

    @property
    def resource_provider_name_from_status(self) -> str|None:
        """Return name of managing ResourceProvider from ResourceClaim status."""
        return self.status.get('provider', {}).get('name')

    @property
    def resource_provider_name_from_spec(self) -> str:
        """Return name of managing ResourceProvider from ResourceClaim spec."""
        return self.spec['provider']['name']

    @property
    def resources(self):
        return self.spec.get('resources', [])

    @property
    def status_resources(self):
        if self.status:
            return self.status.get('resources', [])
        return []

    @property
    def validation_failed(self) -> bool:
        if self.status.get('provider', {}).get('validationErrors'):
            return True
        for resource in self.status_resources:
            if 'validationError' in resource:
                return True
        return False

    async def bind_resource_handle(self,
        logger: kopf.ObjectLogger,
        resource_claim_resources: List[Mapping],
    ):
        resource_handle = await resourcehandle.ResourceHandle.bind_handle_to_claim(
            logger = logger,
            resource_claim = self,
            resource_claim_resources = resource_claim_resources,
        )

        if not resource_handle:
            for provider in await self.get_resource_providers(resource_claim_resources):
                if provider.create_disabled:
                    raise kopf.TemporaryError(
                        f"Found no matching ResourceHandles for {self} and "
                        f"ResourceHandle creation is disabled for {provider}",
                        delay=600
                    )
            resource_handle = await resourcehandle.ResourceHandle.create_for_claim(
                logger = logger,
                resource_claim = self,
                resource_claim_resources = resource_claim_resources,
            )

        status_patch = [{
            "op": "add",
            "path": "/status/resourceHandle",
            "value": resource_handle.reference,
        }]

        lifespan_value = {
            "start": datetime.now(timezone.utc).strftime('%FT%TZ'),
            "end": resource_handle.lifespan_end_timestamp,
            "maximum": resource_handle.get_lifespan_maximum(resource_claim=self),
            "relativeMaximum": resource_handle.get_lifespan_relative_maximum(resource_claim=self),
        }

        if resource_handle.is_ready:
            lifespan_value['firstReady'] = datetime.now(timezone.utc).strftime('%FT%TZ')

        status_patch.append({
            "op": "add",
            "path": "/status/lifespan",
            "value": { k: v for k, v in lifespan_value.items() if v },
        })

        await self.json_patch_status(status_patch)
        logger.info(f"Set {resource_handle} for {self}")

        return resource_handle

    def check_condition(self, when_condition, resource_handle, resource_provider):
        # resource_provider may be None if there is no top-level provider.
        resource_provider_vars = resource_provider.vars if resource_provider else {}
        vars_ = {
            "resource_claim": self,
            "resource_handle": resource_handle,
            "resource_provider": resource_provider,
            "metadata": self.meta,
            "spec": self.spec,
            "status": self.status,
        }
        check_value = recursive_process_template_strings(
            '{{(' + when_condition + ')|bool}}',
            variables = vars_,
            template_variables = { **resource_provider_vars, **resource_handle.vars },
        )
        return check_value

    def check_auto_delete(self, logger, resource_handle, resource_provider) -> bool:
        if not self.auto_delete_when:
            return False
        try:
            check_value = self.check_condition(
                resource_handle=resource_handle,
                resource_provider=resource_provider,
                when_condition=self.auto_delete_when
            )
            return check_value
        except Exception as exception:
            logger.warning(f"Auto delete check failed for {self}: {exception}")
        return False

    def check_auto_detach(self, logger, resource_handle, resource_provider):
        if not self.auto_detach_when:
            return False
        try:
            check_value = self.check_condition(
                resource_handle=resource_handle,
                resource_provider=resource_provider,
                when_condition=self.auto_detach_when
            )
            return check_value
        except Exception as exception:
            logger.warning(f"Auto detach check failed for {self}: {exception}")
        return False

    def get_resource_state_from_status(self, resource_index):
        if not self.status \
        or not 'resources' in self.status \
        or resource_index >= len(self.status['resources']):
            return None
        return self.status['resources'][resource_index].get('state')

    async def set_requested_lifespan_end(self,
        end_datetime,
    ) -> None:
        await self.merge_patch({
            "spec": {
                "lifespan": {
                    "end": end_datetime.strftime('%FT%TZ')
                }
            }
        })

    async def update_status_from_handle(self,
        logger: kopf.ObjectLogger,
        resource_handle: ResourceHandleT,
        resource_states: List[Mapping]|None=None,
    ) -> None:
        async with self.lock:
            logger.debug(f"Updating {self} from {resource_handle}")
            patch = []

            # Reset lifespan from default on first ready
            if resource_handle.is_ready and not self.lifespan_first_ready_timestamp:
                lifespan_default_timedelta = resource_handle.get_lifespan_default_timedelta(resource_claim=self)
                if lifespan_default_timedelta:
                    # Adjust requested end if unchanged from default
                    if not self.requested_lifespan_end_datetime \
                    or lifespan_default_timedelta == self.requested_lifespan_end_datetime - self.lifespan_start_datetime:
                        logger.info(f"Resetting default lifespan end on first ready")
                        await self.set_requested_lifespan_end(
                            datetime.now(timezone.utc) + lifespan_default_timedelta
                        )

            lifespan_maximum = resource_handle.get_lifespan_maximum(resource_claim=self)
            lifespan_relative_maximum = resource_handle.get_lifespan_relative_maximum(resource_claim=self)
            if 'lifespan' in resource_handle.spec \
            and 'lifespan' not in self.status:
                lifespan_value = {
                    "end": resource_handle.lifespan_end_timestamp,
                    "maximum": lifespan_maximum,
                    "relativeMaximum": lifespan_relative_maximum,
                }
                if resource_handle.is_ready:
                    lifespan_value['firstReady'] = datetime.now(timezone.utc).strftime('%FT%TZ')
                patch.append({
                    "op": "add",
                    "path": "/status/lifespan",
                    "value": { k: v for k, v in lifespan_value.items() if v }
                })
            else:
                if self.lifespan_end_timestamp \
                and not resource_handle.lifespan_end_timestamp:
                    patch.append({
                        "op": "remove",
                        "path": "/status/lifespan/end",
                    })
                elif resource_handle.lifespan_end_timestamp \
                and self.lifespan_end_timestamp != resource_handle.lifespan_end_timestamp:
                    patch.append({
                        "op": "add",
                        "path": "/status/lifespan/end",
                        "value": resource_handle.lifespan_end_timestamp,
                    })

                if resource_handle.is_ready and not self.lifespan_first_ready_timestamp:
                    # FIXME - Reset ResourceHandle lifespan default!
                    patch.append({
                        "op": "add",
                        "path": "/status/lifespan/firstReady",
                        "value": datetime.now(timezone.utc).strftime('%FT%TZ'),
                    })

                if lifespan_maximum:
                    if lifespan_maximum != self.lifespan_maximum:
                        patch.append({
                            "op": "add",
                            "path": "/status/lifespan/maximum",
                            "value": lifespan_maximum,
                        })
                elif self.lifespan_maximum:
                    patch.append({
                        "op": "remove",
                        "path": "/status/lifespan/maximum",
                    })

                if lifespan_relative_maximum:
                    if lifespan_relative_maximum != self.lifespan_relative_maximum:
                        patch.append({
                            "op": "add",
                            "path": "/status/lifespan/relativeMaximum",
                            "value": lifespan_relative_maximum,
                        })
                elif self.lifespan_relative_maximum:
                    patch.append({
                        "op": "remove",
                        "path": "/status/lifespan/relativeMaximum",
                    })

            for resource_index, status_resource in enumerate(resource_handle.status_resources):
                if resource_index < len(self.status['resources']):
                    current_entry = self.status['resources'][resource_index]
                    patch_op = "replace"
                else:
                    current_entry = {}
                    patch_op = "add"

                resource_entry = {
                    "provider": resource_handle.resources[resource_index]['provider'],
                    **status_resource,
                }
                if 'name' in current_entry:
                    resource_entry['name'] = current_entry['name']
                if 'validationError' in current_entry:
                    resource_entry['validationError'] = current_entry['validationError']
                if (
                    resource_states and
                    resource_index < len(resource_states) and
                    resource_states[resource_index] is not None
                ):
                    resource_entry['state'] = prune_k8s_resource(
                        resource_states[resource_index]
                    )
                elif 'state' in current_entry:
                    resource_entry['state'] = current_entry['state']

                if resource_entry != current_entry:
                    patch.append({
                        "op": patch_op,
                        "path": f"/status/resources/{resource_index}",
                        "value": resource_entry,
                    })

            for field in ('healthy', 'ready', 'summary'):
                if field in resource_handle.status:
                    if resource_handle.status[field] != self.status.get(field):
                        patch.append({
                            "op": "add",
                            "path": f"/status/{field}",
                            "value": resource_handle.status[field],
                        })
                elif field in self.status:
                    patch.append({
                        "op": "remove",
                        "path": f"/status/{field}",
                    })

            if not patch:
                return

            try:
                await self.json_patch_status(patch)
            except kubernetes_asyncio.client.exceptions.ApiException as exception:
                if exception.status == 404:
                    logger.info(
                        f"Attempt to update status from {resource_handle} on deleted {self}"
                    )
                elif exception.status == 422:
                    logger.warning(f"Failed to apply patch {patch} to {self}")
                else:
                    raise

    async def handle_delete(self, logger: kopf.ObjectLogger):
        if self.has_resource_handle:
            try:
                await Poolboy.custom_objects_api.delete_namespaced_custom_object(
                    group = Poolboy.operator_domain,
                    name = self.resource_handle_name,
                    namespace = self.resource_handle_namespace,
                    plural = 'resourcehandles',
                    version = Poolboy.operator_version,
                )
                logger.info(
                    f"Propagated delete of ResourceClaim {self.name} in {self.namespace} "
                    f"to ResourceHandle {self.resource_handle_name}"
                )
            except kubernetes_asyncio.client.exceptions.ApiException as exception:
                if exception.status != 404:
                    raise

    async def assign_resource_providers(self, logger) -> None:
        """Assign resource providers in status for ResourceClaim with resources listed in spec."""
        resources = self.spec.get('resources', None)

        if not resources:
            raise kopf.TemporaryError(f"{self} has no resources", delay=600)

        providers = []
        for resource in resources:
            provider_name = resource.get('provider', {}).get('name')
            if provider_name:
                provider = await resourceprovider.ResourceProvider.get(provider_name)
            elif 'template' in resource:
                provider = resourceprovider.ResourceProvider.find_provider_by_template_match(resource['template'])
            else:
                raise kopf.TemporaryError(f"ResourceClaim spec.resources require either an explicit provider or a resource template to match.", delay=600)
            providers.append(provider)

        await self.merge_patch_status({
            "resources": [{
                "name": resources[idx].get('name'),
                "provider": {
                    "apiVersion": Poolboy.operator_api_version,
                    "kind": "ResourceProvider",
                    "name": provider.name,
                    "namespace": provider.namespace,
                },
                "state": self.get_resource_state_from_status(resource_index=idx),
            } for idx, provider in enumerate(providers)]
        })

        logger.debug(
            f"Assigned ResourceProviders {', '.join([p.name for p in providers])} "
            f"to ResourceClaim {self.name} in {self.namespace}"
        )

    async def detach(self, resource_handle):
        await self.merge_patch_status({
            "resourceHandle": {
                "detached": True
            }
        })
        await resource_handle.delete()

    async def get_resource_handle(self):
        return await resourcehandle.ResourceHandle.get(self.resource_handle_name)

    async def get_resource_provider(self) -> ResourceProviderT:
        """Return ResourceProvider configured to manage ResourceClaim."""
        return await resourceprovider.ResourceProvider.get(self.resource_provider_name)

    async def get_resource_providers(self, resources:List[Mapping]|None=None) -> List[ResourceProviderT]:
        """Return list of ResourceProviders assigned to each resource entry of ResourceClaim."""
        resource_providers = []
        if resources == None:
            resources = self.status.get('resources', [])
        for resource in resources:
            resource_providers.append(
                await resourceprovider.ResourceProvider.get(resource['provider']['name'])
            )
        return resource_providers

    async def get_resources_from_provider(self, resource_handle: ResourceHandleT|None=None) -> List[Mapping]:
        """Return resources for this claim as defined by ResourceProvider"""
        resource_provider = await self.get_resource_provider()
        return await resource_provider.get_resources(
            resource_claim = self,
            resource_handle = resource_handle,
        )

    async def initialize_claim(self, logger):
        """Set defaults into resource templates in claim and set init timestamp."""
        claim_status_resources = self.status.get('resources', [])
        patch = {
            "metadata": {
                "annotations": {
                    f"{Poolboy.operator_domain}/resource-claim-init-timestamp": datetime.now(timezone.utc).strftime('%FT%TZ')
                }
            },
        }

        claim_resources = self.spec.get('resources')
        if claim_resources:
            patch['spec'] = {
                "resources": claim_resources
            }
            for i, claim_resource in enumerate(claim_resources):
                claim_status_resource = claim_status_resources[i]
                provider_ref = claim_status_resource['provider']
                provider = await resourceprovider.ResourceProvider.get(provider_ref['name'])
                claim_resource['provider'] = provider_ref
                template_with_defaults = provider.apply_template_defaults(
                    resource_claim = self,
                    resource_index = i,
                )
                if template_with_defaults:
                    claim_resource['template'] = template_with_defaults

        await self.merge_patch(patch)
        logger.info(f"ResourceClaim {self.name} in {self.namespace} initialized")

    async def manage(self, logger) -> None:
        async with self.lock:
            if self.lifespan_start_datetime \
            and self.lifespan_start_datetime > datetime.now(timezone.utc):
                return

            if self.is_detached:
                # Normally lifespan end is tracked by the ResourceHandle.
                # Detached ResourceClaims have no handle.
                if self.lifespan_end_datetime \
                and self.lifespan_end_datetime < datetime.now(timezone.utc):
                    logger.info(f"Deleting detached {self} at end of lifespan")
                    await self.delete()
                # No further processing for detached ResourceClaim
                return

            resource_provider = None
            if self.has_resource_provider:
                if self.has_spec_resources:
                    raise kopf.TemporaryError(
                        f"{self} has both spec.provider and spec.resources!",
                        delay = 600
                    )
                if not 'provider' in self.status:
                    await self.merge_patch_status({
                        "provider": {
                            "name": self.resource_provider_name_from_spec
                        }
                    })
                elif self.resource_provider_name_from_spec != self.resource_provider_name_from_status:
                    raise kopf.TemporaryError(
                        f"spec.provider value ({self.resource_provider_name_from_spec}) does not "
                        f"match status.provider value ({self.resource_provider_name_from_status})! "
                        f"spec.provider is immutable!",
                        delay = 600
                    )

                try:
                    resource_provider = await self.get_resource_provider()
                except kubernetes_asyncio.client.exceptions.ApiException as e:
                    if e.status == 404:
                        raise kopf.TemporaryError(
                            f"ResourceProvider ({self.resource_provider_name_from_spec}) not found",
                            delay = 600
                        )
                    else:
                        raise

                metadata_patch = {}
                for key, value in resource_provider.resource_claim_annotations.items():
                    if self.annotations.get(key) != value:
                        metadata_patch.setdefault('annotations', {})[key] = value
                for key, value in resource_provider.resource_claim_labels.items():
                    if self.labels.get(key) != value:
                        metadata_patch.setdefault('labels', {})[key] = value
                if metadata_patch:
                    await self.merge_patch({
                        "metadata": metadata_patch,
                    })

                if resource_provider.approval_required:
                    if not 'approval' in self.status:
                        await self.merge_patch_status({
                            "approval": {
                                "message": resource_provider.approval_pending_message,
                                "state": "pending"
                            }
                        })
                        return
                    elif not self.is_approved:
                        return


            elif self.has_spec_resources:
                if not self.have_resource_providers:
                    await self.assign_resource_providers(logger=logger)

            else:
                raise kopf.TemporaryError(
                    f"{self} has neither spec.provider nor spec.resources!",
                    delay = 600
                )

            if not self.claim_is_initialized:
                await self.initialize_claim(logger=logger)

            if self.has_resource_handle:
                try:
                    resource_handle = await self.get_resource_handle()
                except kubernetes_asyncio.client.exceptions.ApiException as e:
                    if e.status == 404:
                        logger.info(f"Found {self.resource_handle_description} deleted, propagating to {self}")
                        await self.delete()
                        return
                    else:
                        raise
            else:
                resource_handle = None

            await self.validate(logger=logger, resource_handle=resource_handle)
            if self.validation_failed:
                return

            if self.has_resource_provider:
                resource_claim_resources = await self.get_resources_from_provider()
            else:
                resource_claim_resources = self.resources

            if not resource_handle:
                resource_handle = await self.bind_resource_handle(
                    logger = logger,
                    resource_claim_resources = resource_claim_resources,
                )
            if resource_handle.ignore:
                return

            if self.check_auto_delete(logger=logger, resource_handle=resource_handle, resource_provider=resource_provider):
                logger.info(f"auto-delete of {self} triggered")
                await self.delete()
                return

            if self.check_auto_detach(logger=logger, resource_handle=resource_handle, resource_provider=resource_provider):
                logger.info(f"auto-detach of {self} triggered")
                await self.detach(resource_handle=resource_handle)
                return

            await self.__manage_resource_handle(
                logger = logger,
                resource_claim_resources = resource_claim_resources,
                resource_handle = resource_handle,
            )

    async def __manage_resource_handle(self,
        logger: kopf.ObjectLogger,
        resource_claim_resources: List[Mapping],
        resource_handle: ResourceHandleT|None,
    ) -> None:
        patch = []

        # Ensure ResourceHandle provider matches ResourceClaim
        if self.has_resource_provider:
            if resource_handle.spec.get('provider') != self.spec['provider']:
                logger.info(f"Setting provider on {resource_handle}")
                patch.append({
                    "op": "add",
                    "path": "/spec/provider",
                    "value": self.spec['provider']
                })
        elif resource_handle.has_resource_provider:
            logger.info(f"Removing provider from {resource_handle}")
            patch.append({
                "op": "remove",
                "path": "/spec/provider",
            })

        # Add any new resources from claim to handle
        for resource_index in range(len(resource_handle.resources), len(resource_claim_resources)):
            resource = resource_claim_resources[resource_index]
            logger.info(
                f"Adding new resource from {self} to {resource_handle} "
                f"with ResourceProvider {resource['provider']['name']}"
            )
            patch.append({
                "op": "add",
                "path": f"/spec/resources/{resource_index}",
                "value": {
                    "provider": resource['provider'],
                }
            })

        # Add changes from claim resource template to resource handle template
        for resource_index, claim_resource in enumerate(resource_claim_resources):
            claim_resource_template = claim_resource.get('template')
            if not claim_resource_template:
                continue
            if resource_index < len(resource_handle.spec['resources']):
                handle_resource_template = resource_handle.spec['resources'][resource_index].get('template', {})
            else:
                handle_resource_template = {}
            merged_resource_template = deepcopy(handle_resource_template)
            deep_merge(merged_resource_template, claim_resource_template)
            if handle_resource_template != merged_resource_template:
                patch.append({
                    "op": "add",
                    "path": f"/spec/resources/{resource_index}/template",
                    "value": merged_resource_template,
                })

        set_lifespan_end = self.requested_lifespan_end_datetime
        if set_lifespan_end:
            maximum_datetime = resource_handle.get_lifespan_end_maximum_datetime(resource_claim=self)
            if maximum_datetime \
            and set_lifespan_end > maximum_datetime:
                logger.warning(
                    f"{self} specifies lifespan end of "
                    f"{set_lifespan_end.strftime('%FT%TZ')} which exceeds "
                    f"{resource_handle} maximum of {maximum_datetime.strftime('%FT%TZ')}"
                )
                set_lifespan_end = maximum_datetime

            set_lifespan_end_timestamp = set_lifespan_end.strftime('%FT%TZ')

            if not 'lifespan' in resource_handle.spec:
                logger.info(f"Setting lifespan end for {resource_handle} to {set_lifespan_end_timestamp}")
                patch.append({
                    "op": "add",
                    "path": "/spec/lifespan",
                    "value": {
                        "end": set_lifespan_end_timestamp,
                    }
                })
            elif set_lifespan_end != resource_handle.lifespan_end_datetime:
                logger.info(f"Changing lifespan end for {resource_handle} to {set_lifespan_end_timestamp}")
                patch.append({
                    "op": "add",
                    "path": "/spec/lifespan/end",
                    "value": set_lifespan_end_timestamp,
                })

        if patch:
            await resource_handle.json_patch(patch)

    async def refetch(self) -> ResourceClaimT|None:
        try:
            definition = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                Poolboy.operator_domain, Poolboy.operator_version, self.namespace, 'resourceclaims', self.name
            )
            self.refresh_from_definition(definition)
            return self
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404:
                self.unregister(name=self.name, namespace=self.namespace)
                return None
            raise

    async def validate(self,
        logger: kopf.ObjectLogger,
        resource_handle: ResourceHandleT|None
    ) -> None:
        if self.has_resource_provider:
            await self.validate_with_provider(logger=logger, resource_handle=resource_handle)
        elif self.has_spec_resources:
            await self.validate_resources(logger=logger, resource_handle=resource_handle)

    async def validate_resources(self,
        logger: kopf.ObjectLogger,
        resource_handle: ResourceHandleT|None
    ) -> None:
        """Validate ResourceClaim with explicit list of resources.
        Patch status with failure details if validation fails.
        """
        resources = self.spec.get('resources', [])
        resource_providers = await self.get_resource_providers()
        status_json_patch = []

        for i, resource in enumerate(resources):
            resource_provider = resource_providers[i]
            resource_template = resource.get('template', {})
            status_resource = self.status['resources'][i]
            try:
                resource_providers[i].validate_resource_template(
                    resource_claim = self,
                    resource_handle = resource_handle,
                    template = resource.get('template', {}),
                )
                if 'validationError' in status_resource:
                    logger.info(f"Clearing validation error for {self}")
                    status_json_patch.append({
                        "op": "remove",
                        "path": f"/status/resources/{i}/validationError",
                    })
            except Exception as exception:
                logger.warning(f"Validation failed for {self}: {exception}")
                status_json_patch.append({
                    "op": "add",
                    "path": f"/status/resources/{i}/validationError",
                    "value": str(exception),
                })
        if status_json_patch:
            await self.json_patch_status(status_json_patch)

    async def validate_with_provider(self,
        logger: kopf.ObjectLogger,
        resource_handle: ResourceHandleT|None
    ) -> None:
        """Validate ResourceClaim using its ResourceProvider.
        Patch status with failure details if validation fails.
        """
        resource_handle_vars = resource_handle.vars if resource_handle else {}
        try:
            resource_provider = await self.get_resource_provider()
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404:
                raise kopf.TemporaryError(
                    f"ResourceProvider {self.resource_provider_name} not found.",
                    delay=600
                )
            raise

        validation_errors = []
        vars_ = {
            "resource_claim": self,
            "resource_handle": resource_handle,
            "resource_provider": resource_provider,
        }

        parameter_values = self.spec.get('provider', {}).get('parameterValues', {})
        parameter_states = self.status.get('provider', {}).get('parameterValues')

        # Collect parameter values from status and resource provider defaults
        parameters_from_defaults = set()
        for parameter in resource_provider.get_parameters():
            if parameter.name not in parameter_values:
                if parameter_states and parameter.name in parameter_states:
                    parameter_values[parameter.name] = parameter_states[parameter.name]
                elif parameter.default_template is not None:
                    parameters_from_defaults.add(parameter.name)
                    parameter_values[parameter.name] = recursive_process_template_strings(
                        parameter.default_template,
                        variables = { **vars_, **parameter_values },
                        template_variables = {**resource_provider.vars, **resource_handle_vars},
                    )
                elif parameter.default_value is not None:
                    parameters_from_defaults.add(parameter.name)
                    parameter_values[parameter.name] = parameter.default_value

        parameter_names = set()
        for parameter in resource_provider.get_parameters():
            parameter_names.add(parameter.name)
            if parameter.name in parameter_values:
                value = parameter_values[parameter.name]
                if parameter_states is not None:
                    if value == parameter_states.get(parameter.name):
                        # Unchanged from current state is automatically considered valid
                        # even if validation rules have changed.
                        continue
                    if parameter.name in parameters_from_defaults:
                        # New parameter set from default is automatically valid
                        continue
                    if not parameter.allow_update:
                        validation_errors.append(f"Parameter {parameter.name} is immutable.")
                        continue

                if parameter.open_api_v3_schema_validator:
                    try:
                        parameter.open_api_v3_schema_validator.validate(value)
                    except Exception as exception:
                        validation_errors.append(f"Parameter {parameter.name} schema validation failed: {exception}")
                        continue

                for validation_check in parameter.validation_checks:
                    try:
                        check_successful = recursive_process_template_strings(
                            '{{(' + validation_check.check + ')|bool}}',
                            variables = { **vars_, **parameter_values, "value": value },
                            template_variables = {**resource_provider.vars, **resource_handle_vars},
                        )
                        if not check_successful:
                            validation_errors.append(f"Parameter {parameter.name} failed check: {validation_check.name}")
                    except Exception as exception:
                        validation_errors.append(f"Parameter {parameter.name} exception on check {validation_check.name}: {exception}")
            elif parameter.required:
                validation_errors.append(f"Parameter {parameter.name} is required.")

        for name, value in parameter_values.items():
            if parameter_states and value == parameter_states.get(name):
                # Unchanged from current state is automatically considered valid
                # even if parameter has been removed.
                continue
            if name not in parameter_names:
                validation_errors.append(f"No such parameter {name}.")

        if validation_errors:
            if validation_errors == self.status.get('provider', {}).get('validationErrors'):
                return
            logger.info(f"Validation failed with {validation_errors}")
            await self.merge_patch_status({
                "provider": {
                    "name": resource_provider.name,
                    "validationErrors": validation_errors,
                }
            })
            return

        if parameter_values != parameter_states or 'validationErrors' in self.status.get('provider', {}):
            patch = {
                "provider": {
                    "name": resource_provider.name,
                    "parameterValues": parameter_values,
                    "validationErrors": None,
                }
            }

            if 'resources' not in self.status:
                resources = await resource_provider.get_resources(
                    parameter_values = parameter_values,
                    resource_claim = self,
                )
                patch['resources'] = [
                    {
                        "name": resource['name'],
                        "provider": resource['provider'],
                    } for resource in resources
                ]
            await self.merge_patch_status(patch)
