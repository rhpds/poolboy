import asyncio
import inflection
import kopf
import kubernetes_asyncio
import logging

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, TypeVar

from base64 import urlsafe_b64encode
from hashlib import sha256

import poolboy_k8s

from kopfobject import KopfObject
from poolboy import Poolboy
import resourcehandle
import resourceprovider

logger = logging.getLogger('resource_watch')

class ResourceWatchFailedError(Exception):
    pass

class ResourceWatchRestartError(Exception):
    pass

ResourceHandleT = TypeVar('ResourceHandleT', bound='ResourceHandle')
ResourceWatchT = TypeVar('ResourceWatchT', bound='ResourceWatch')

class ResourceWatch(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourceWatch"
    plural = "resourcewatches"

    instances = {}
    class_lock = asyncio.Lock()

    class CacheEntry:
        def __init__(self, resource: Mapping):
            self.resource = resource
            self.cache_datetime = datetime.now(timezone.utc)

        @property
        def is_expired(self):
            return (datetime.now(timezone.utc) - self.cache_datetime).total_seconds() > Poolboy.resource_refresh_interval

    @classmethod
    def __instance_key(cls, api_version: str, kind: str, namespace: str|None) -> str:
        """Return cache key used to identify ResourceWatch in instances dict"""
        return "|".join((api_version, kind, namespace or '*'))

    @classmethod
    def __make_name(cls,
        api_version: str,
        kind: str,
        namespace: str|None,
    ):
        """Return unique name for ResourceWatch determined by watch target.
        This hash prevents race conditions when otherwise multiple watches might be created."""
        return (namespace or 'cluster') + '-' + urlsafe_b64encode(
            sha256(':'.join((api_version,kind,namespace or '')).encode('utf-8'))
            .digest()
            ).decode('utf-8').replace('=', '').replace('-', '').replace('_', '').lower()[:12]

    @classmethod
    def __get_instance(cls,
        api_version: str,
        kind: str,
        namespace: str|None,
    ):
        """Return ResourceWatch from instances dict."""
        return cls.instances.get(
            cls.__instance_key(
                api_version=api_version,
                kind=kind,
                namespace=namespace
            )
        )

    @classmethod
    def __register_definition(cls, definition: Mapping) -> ResourceWatchT:
        resource_watch = cls.__get_instance(
            api_version=definition['spec']['apiVersion'],
            kind=definition['spec']['kind'],
            namespace=definition['spec'].get('namespace'),
        )
        if resource_watch:
            resource_watch.refresh_from_definition(definition=definition)
        else:
            resource_watch = cls(
                annotations=definition['metadata'].get('annotations', {}),
                labels=definition['metadata'].get('labels', {}),
                meta=definition['metadata'],
                name=definition['metadata']['name'],
                namespace=Poolboy.namespace,
                spec=definition['spec'],
                status=definition.get('status', {}),
                uid=definition['metadata']['uid'],
            )
        resource_watch.__register()
        return resource_watch

    @classmethod
    async def create_as_needed(cls,
        api_version: str,
        kind: str,
        namespace: str|None,
    ) -> ResourceWatchT|None:
        async with cls.class_lock:
            resource_watch = await cls.__get(
                api_version=api_version,
                kind=kind,
                namespace=namespace,
            )
            if resource_watch:
                return resource_watch

            name = cls.__make_name(
                api_version=api_version,
                kind=kind,
                namespace=namespace,
            )

            definition = {
                "apiVersion": '/'.join((cls.api_group, cls.api_version)),
                "kind": cls.kind,
                "metadata": {
                    "name": name,
                },
                "spec": {
                    "apiVersion": api_version,
                    "kind": kind,
                }
            }
            if namespace:
                definition['spec']['namespace'] = namespace

            try:
                definition = await Poolboy.custom_objects_api.create_namespaced_custom_object(
                    group = cls.api_group,
                    namespace = Poolboy.namespace,
                    plural = cls.plural,
                    version = cls.api_version,
                    body = definition,
                )
                resource_watch = cls.from_definition(definition)
                logger.info(f"Created {resource_watch}")
                return resource_watch
            except kubernetes_asyncio.client.exceptions.ApiException as exception:
                # Okay if already exists
                if exception.status == 409:
                    return None
                else:
                    raise

    @classmethod
    async def get(cls,
        api_version: str,
        kind: str,
        namespace: str|None,
    ) -> ResourceWatchT:
        """Get ResourceWatch by watched resources"""
        async with cls.class_lock:
            return await cls.__get(
                api_version=api_version,
                kind=kind,
                namespace=namespace,
            )

    @classmethod
    async def __get(cls,
        api_version: str,
        kind: str,
        namespace: str|None,
    ) -> ResourceWatchT:
        resource_watch = cls.__get_instance(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
        )
        if resource_watch:
            return resource_watch

        name = cls.__make_name(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
        )

        try:
            list_object = await Poolboy.custom_objects_api.get_namespaced_custom_object(
                group = cls.api_group,
                name = name,
                namespace = Poolboy.namespace,
                plural = cls.plural,
                version = cls.api_version,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404:
                return None
            else:
                raise

    @classmethod
    async def get_resource_from_any(cls,
        api_version: str,
        kind: str,
        name: str,
        namespace: str|None,
        not_found_okay: bool=False,
        use_cache: bool=True,
    ) -> Mapping|None:
        # Try to get from other watch object
        watch = cls.__get_instance(
            api_version=api_version,
            kind=kind,
            namespace=namespace,
        )
        if watch:
            return await watch.get_resource(
                name=name,
                not_found_okay=not_found_okay,
            )
        # Fall back to attempt fetch directly
        try:
            return await poolboy_k8s.get_object(
                api_version=api_version,
                kind=kind,
                name=name,
                namespace=namespace,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404 and not_found_okay:
                return None
            else:
                raise

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
    ) -> ResourceWatchT:
        async with cls.class_lock:
            resource_watch = cls.__get_instance(
                api_version=spec['apiVersion'],
                kind=spec['kind'],
                namespace=spec.get('namespace')
            )
            if resource_watch:
                resource_watch.refresh(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
            else:
                resource_watch = cls(
                    annotations = annotations,
                    labels = labels,
                    meta = meta,
                    name = name,
                    namespace = namespace,
                    spec = spec,
                    status = status,
                    uid = uid,
                )
                resource_watch.__register()
            return resource_watch

    @classmethod
    async def stop_all(cls) -> None:
        """Stop all ResourceWatch tasks"""
        async with cls.class_lock:
            tasks = []
            for resource_watch in cls.instances.values():
                if resource_watch.task is not None:
                    resource_watch.task.cancel()
                    tasks.append(resource_watch.task)
            if tasks:
                await asyncio.gather(*tasks)

    def __init__(self,
        annotations: kopf.Annotations|Mapping,
        labels: kopf.Labels|Mapping,
        meta: kopf.Meta|Mapping,
        name: str,
        namespace: str,
        spec: kopf.Spec|Mapping,
        status: kopf.Status|Mapping,
        uid: str,
    ):
        super().__init__(
            annotations=annotations,
            labels=labels,
            meta=meta,
            name=name,
            namespace=namespace,
            spec=spec,
            status=status,
            uid=uid,
        )
        # Cache to store fetched resources
        self.cache = {}
        # Task for when watch is running
        self.task = None

    def __register(self) -> None:
        """
        Add ResourceWatch to register of instances.
        """
        self.instances[self.__self_instance_key] = self

    def __str__(self) -> str:
        return (
            f"{self.kind} {self.name} ({self.watch_api_version} {self.watch_kind} in {self.watch_namespace})"
            if self.watch_namespace else
            f"{self.kind} {self.name} ({self.watch_api_version} {self.watch_kind})"
        )

    def __self_instance_key(self) -> str:
        return self.__instance_key(
            api_version=self.api_version,
            kind=self.kind,
            namespace=self.namespace,
        )

    @property
    def name_hash(self) -> str:
        return self.name.rsplit('-', 1)[1]

    @property
    def watch_api_version(self) -> str:
        return self.spec['apiVersion']

    @property
    def watch_kind(self) -> str:
        return self.spec['kind']

    @property
    def watch_namespace(self) -> str|None:
        return self.spec.get('namespace')

    def cache_clean(self):
        self.cache = {
            name: cache_entry
            for name, cache_entry in self.cache.items()
            if not cache_entry.is_expired
        }

    async def create_pod(self,
        logger: kopf.ObjectLogger,
    ) -> None:
        replicaset = kubernetes_asyncio.client.V1ReplicaSet(
            api_version="apps/v1",
            kind="ReplicaSet",
            metadata=kubernetes_asyncio.client.V1ObjectMeta(
                name=f"{Poolboy.manager_pod.metadata.name}-watch-{self.name_hash}",
                namespace=Poolboy.namespace,
                owner_references=[
                    kubernetes_asyncio.client.V1OwnerReference(
                        api_version=Poolboy.manager_pod.api_version,
                        controller=True,
                        kind=Poolboy.manager_pod.kind,
                        name=Poolboy.manager_pod.metadata.name,
                        uid=Poolboy.manager_pod.metadata.uid,
                    )
                ]
            ),
        )
        replicaset.spec = kubernetes_asyncio.client.V1ReplicaSetSpec(
            replicas=1,
            selector=kubernetes_asyncio.client.V1LabelSelector(
                match_labels={
                    "app.kubernetes.io/name": Poolboy.manager_pod.metadata.name,
                    "app.kubernetes.io/instance": f"watch-{self.name_hash}",
                },
            ),
            template=kubernetes_asyncio.client.V1PodTemplateSpec(
                metadata=kubernetes_asyncio.client.V1ObjectMeta(
                    labels={
                        "app.kubernetes.io/name": Poolboy.manager_pod.metadata.name,
                        "app.kubernetes.io/instance": f"watch-{self.name_hash}",
                    },
                ),
                spec=deepcopy(Poolboy.manager_pod.spec),
            ),
        )

        replicaset.spec.template.spec.containers[0].env = [
            env_var
            for env_var in Poolboy.manager_pod.spec.containers[0].env
            if env_var.name not in {
                'OPERATOR_MODE',
                'RESOURCE_HANDLER_COUNT',
                'RESOURCE_HANDLER_RESOURCES',
                'RESOURCE_WATCH_RESOURCES',
            }
        ]
        replicaset.spec.template.spec.containers[0].env.append(
            kubernetes_asyncio.client.V1EnvVar(
                name='OPERATOR_MODE',
                value='resource-watch',
            )
        )
        replicaset.spec.template.spec.containers[0].env.append(
            kubernetes_asyncio.client.V1EnvVar(
                name='WATCH_NAME',
                value=self.name,
            )
        )
        replicaset.spec.template.spec.node_name = None
        if Poolboy.resource_watch_resources:
            replicaset.spec.template.spec.containers[0].resources = kubernetes_asyncio.client.V1ResourceRequirements(
                limits=Poolboy.resource_watch_resources.get('limits'),
                requests=Poolboy.resource_watch_resources.get('requests'),
            )

        replicaset = await Poolboy.apps_v1_api.create_namespaced_replica_set(
            namespace=Poolboy.namespace,
            body=replicaset,
        )
        logger.info(f"Created ReplicaSet {replicaset.metadata.name} for {self}")

    async def get_resource(self,
        name: str,
        not_found_okay: bool=False,
        use_cache: bool=True,
    ) -> Mapping|None:
        if use_cache:
            cache_entry = self.cache.get(name)
            if cache_entry:
                if cache_entry.is_expired:
                    self.cache.pop(name, None)
                else:
                    return cache_entry.resource
        try:
            resource = await poolboy_k8s.get_object(
                api_version=self.watch_api_version,
                kind=self.watch_kind,
                name=name,
                namespace=self.watch_namespace,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404 and not_found_okay:
                return None
            else:
                raise
        if use_cache and resource:
            self.cache[name] = ResourceWatch.CacheEntry(resource)
        return resource

    async def start(self, logger) -> None:
        logger.info(f"Starting {self}")
        self.task = asyncio.create_task(self.watch())

    async def watch(self):
        try:
            if '/' in self.watch_api_version:
                group, version = self.watch_api_version.split('/')
                plural = await poolboy_k8s.kind_to_plural(group=group, version=version, kind=self.watch_kind)
                kwargs = {"group": group, "plural": plural, "version": version}
                if self.watch_namespace:
                    method = Poolboy.custom_objects_api.list_namespaced_custom_object
                    kwargs['namespace'] = self.watch_namespace
                else:
                    method = Poolboy.custom_objects_api.list_cluster_custom_object
            elif self.watch_namespace:
                method = getattr(
                    Poolboy.core_v1_api, "list_namespaced_" + inflection.underscore(self.watch_kind)
                )
                kwargs = {"namespace": self.watch_namespace}
            else:
                method = getattr(
                    Poolboy.core_v1_api, "list_" + inflection.underscore(self.watch_kind)
                )
                kwargs = {}

            while True:
                watch_start_dt = datetime.now(timezone.utc)
                try:
                    await self.__watch(method, **kwargs)
                except asyncio.CancelledError:
                    logger.debug(f"{self} cancelled")
                    return
                except ResourceWatchRestartError as e:
                    logger.debug(f"{self} restart: {e}")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 10:
                        await asyncio.sleep(10 - watch_duration)
                except ResourceWatchFailedError as e:
                    logger.warning(f"{self} failed: {e}")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 60:
                        await asyncio.sleep(60 - watch_duration)
                except Exception as e:
                    logger.exception(f"{self} exception")
                    watch_duration = (datetime.now(timezone.utc) - watch_start_dt).total_seconds()
                    if watch_duration < 60:
                        await asyncio.sleep(60 - watch_duration)
                logger.debug(f"Restarting {self}")

        except asyncio.CancelledError:
            return

    async def __watch(self, method, **kwargs):
        watch = None
        self.cache_clean()
        try:
            watch = kubernetes_asyncio.watch.Watch()
            async for event in watch.stream(method, **kwargs):
                if not isinstance(event, Mapping):
                    raise ResourceWatchFailedError(f"UNKNOWN EVENT: {event}")

                event_obj = event['object']
                event_type = event['type']
                if not isinstance(event_obj, Mapping):
                    event_obj = Poolboy.api_client.sanitize_for_serialization(event_obj)
                if event_type == 'ERROR':
                    if event_obj['kind'] == 'Status':
                        if event_obj['reason'] in ('Expired', 'Gone'):
                            raise ResourceWatchRestartError(event_obj['reason'].lower())
                        else:
                            raise ResourceWatchFailedError(f"{event_obj['reason']} {event_obj['message']}")
                    else:
                        raise ResourceWatchFailedError(f"UNKNOWN EVENT: {event}")
                try:
                    await self.__watch_event(event_type=event_type, event_obj=event_obj)
                except Exception:
                    logger.exception(f"Error handling {event}")
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 410:
                raise ResourceWatchRestartError("Received 410 expired response.")
            else:
                raise
        finally:
            if watch:
                await watch.close()

    async def __watch_event(self, event_type, event_obj):
        event_obj_annotations = event_obj['metadata'].get('annotations')
        if not event_obj_annotations:
            return
        if event_obj_annotations.get(Poolboy.resource_handle_deleted_annotation) == 'true':
            return

        resource_handle_name = event_obj_annotations.get(Poolboy.resource_handle_name_annotation)
        resource_index = int(event_obj_annotations.get(Poolboy.resource_index_annotation, 0))
        resource_name = event_obj['metadata']['name']
        resource_namespace = event_obj['metadata'].get('namespace')
        resource_description = (
            f"{event_obj['apiVersion']} {event_obj['kind']} {resource_name} in {resource_namespace}"
            if resource_namespace else
            f"{event_obj['apiVersion']} {event_obj['kind']} {resource_name}"
        )

        if not resource_handle_name:
            return

        try:
            resource_handle = await resourcehandle.ResourceHandle.get(
                name=resource_handle_name,
                use_cache=Poolboy.operator_mode_all_in_one,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404:
                logger.warning(f"ResourceHandle {resource_handle_name} not found for event on {resource_description}")
            else:
                logger.exception(
                    f"Failed to get ResourceHandle {resource_handle_name} for event on {resource_description}"
                )
            return

        # Don't update ResourceHandle marked for ignore
        if not resource_handle or resource_handle.ignore or resource_handle.is_deleting:
            return

        # Ignore updates to claimed ResourceHandles marked to ignore
        resource_claim = await resource_handle.get_resource_claim(
            not_found_okay=True,
        )
        if resource_claim and resource_claim.ignore:
            return

        # Get full list of resources to update ResourceHandle status
        resource_states = []
        for (idx, resource) in enumerate(resource_handle.status_resources):
            if idx == resource_index:
                resource_states.append(event_obj)
                continue
            reference = resource.get('reference')
            if reference:
                if(
                    reference['apiVersion'] == self.watch_api_version and
                    reference['kind'] == self.watch_kind and
                    reference.get('namespace') == self.watch_namespace
                ):
                    resource_states.append(
                        await self.get_resource(
                            name=reference['name'],
                            not_found_okay=True,
                        )
                    )
                else:
                    resource_states.append(
                        await self.get_resource_from_any(
                            api_version=reference['apiVersion'],
                            kind=reference['kind'],
                            name=reference['name'],
                            namespace=reference.get('namespace'),
                            not_found_okay=True,
                        )
                    )
            else:
                resource_states.append(None)

        try:
            await resource_handle.update_status(
                logger=logger,
                resource_claim=resource_claim,
                resource_states=resource_states,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            logger.exception(
                f"Failed updating status on {resource_handle} from event on {resource_description}"
            )
