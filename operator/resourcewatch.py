import asyncio
import logging
from base64 import urlsafe_b64encode
from datetime import datetime, timezone
from hashlib import sha256
from typing import Mapping, TypeVar

import inflection
import kopf
import kubernetes_asyncio
import poolboy_k8s
import resourcehandle
from cache import Cache, CacheTag
from kopfobject import KopfObject
from poolboy import Poolboy

logger = logging.getLogger("resource_watch")


class ResourceWatchFailedError(Exception):
    pass


class ResourceWatchRestartError(Exception):
    pass


ResourceHandleT = TypeVar("ResourceHandleT", bound="ResourceHandle")
ResourceWatchT = TypeVar("ResourceWatchT", bound="ResourceWatch")


class ResourceWatch(KopfObject):
    api_group = Poolboy.operator_domain
    api_version = Poolboy.operator_version
    kind = "ResourceWatch"
    plural = "resourcewatches"

    class_lock = asyncio.Lock()

    @classmethod
    def __instance_key(cls, api_version: str, kind: str, namespace: str | None) -> str:
        """Return cache key used to identify ResourceWatch in instances dict"""
        return "|".join((api_version, kind, namespace or "*"))

    @classmethod
    def __make_name(
        cls,
        api_version: str,
        kind: str,
        namespace: str | None,
    ):
        """Return unique name for ResourceWatch determined by watch target.
        This hash prevents race conditions when otherwise multiple watches might be created."""
        return (
            (namespace or "cluster")
            + "-"
            + urlsafe_b64encode(
                sha256(
                    ":".join((api_version, kind, namespace or "")).encode("utf-8")
                ).digest()
            )
            .decode("utf-8")
            .replace("=", "")
            .replace("-", "")
            .replace("_", "")
            .lower()[:12]
        )

    @classmethod
    def __get_instance(
        cls,
        api_version: str,
        kind: str,
        namespace: str | None,
    ):
        """Return ResourceWatch from cache."""
        instance_key = cls.__instance_key(
            api_version=api_version, kind=kind, namespace=namespace
        )
        return cls.cache_get(CacheTag.WATCH, instance_key)

    @classmethod
    def __register_definition(cls, definition: Mapping) -> ResourceWatchT:
        resource_watch = cls.__get_instance(
            api_version=definition["spec"]["apiVersion"],
            kind=definition["spec"]["kind"],
            namespace=definition["spec"].get("namespace"),
        )
        if resource_watch:
            resource_watch.refresh_from_definition(definition=definition)
        else:
            resource_watch = cls(
                annotations=definition["metadata"].get("annotations", {}),
                labels=definition["metadata"].get("labels", {}),
                meta=definition["metadata"],
                name=definition["metadata"]["name"],
                namespace=Poolboy.namespace,
                spec=definition["spec"],
                status=definition.get("status", {}),
                uid=definition["metadata"]["uid"],
            )
        resource_watch.__register()
        return resource_watch

    @classmethod
    async def create_as_needed(
        cls,
        api_version: str,
        kind: str,
        namespace: str | None,
    ) -> ResourceWatchT | None:
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
                "apiVersion": "/".join((cls.api_group, cls.api_version)),
                "kind": cls.kind,
                "metadata": {
                    "name": name,
                },
                "spec": {
                    "apiVersion": api_version,
                    "kind": kind,
                },
            }
            if namespace:
                definition["spec"]["namespace"] = namespace

            try:
                definition = (
                    await Poolboy.custom_objects_api.create_namespaced_custom_object(
                        group=cls.api_group,
                        namespace=Poolboy.namespace,
                        plural=cls.plural,
                        version=cls.api_version,
                        body=definition,
                    )
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
    async def get(
        cls,
        api_version: str,
        kind: str,
        namespace: str | None,
    ) -> ResourceWatchT:
        """Get ResourceWatch by watched resources"""
        async with cls.class_lock:
            return await cls.__get(
                api_version=api_version,
                kind=kind,
                namespace=namespace,
            )

    @classmethod
    async def __get(
        cls,
        api_version: str,
        kind: str,
        namespace: str | None,
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
                group=cls.api_group,
                name=name,
                namespace=Poolboy.namespace,
                plural=cls.plural,
                version=cls.api_version,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404:
                return None
            else:
                raise

    @classmethod
    async def get_resource_from_any(
        cls,
        api_version: str,
        kind: str,
        name: str,
        namespace: str | None,
        not_found_okay: bool = False,
        use_cache: bool = True,
    ) -> Mapping | None:
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
                api_version=spec["apiVersion"],
                kind=spec["kind"],
                namespace=spec.get("namespace"),
            )
            if resource_watch:
                resource_watch.refresh(
                    annotations=annotations,
                    labels=labels,
                    meta=meta,
                    spec=spec,
                    status=status,
                    uid=uid,
                )
            else:
                resource_watch = cls(
                    annotations=annotations,
                    labels=labels,
                    meta=meta,
                    name=name,
                    namespace=namespace,
                    spec=spec,
                    status=status,
                    uid=uid,
                )
                resource_watch.__register()
            return resource_watch

    @classmethod
    async def stop_all(cls) -> None:
        """Stop all ResourceWatch tasks"""
        async with cls.class_lock:
            tasks = []
            for instance_key in Cache.get_keys_by_tag(CacheTag.WATCH):
                resource_watch = cls.cache_get(CacheTag.WATCH, instance_key)
                if resource_watch and resource_watch.task is not None:
                    resource_watch.task.cancel()
                    tasks.append(resource_watch.task)
            if tasks:
                await asyncio.gather(*tasks)

    def __init__(
        self,
        annotations: kopf.Annotations | Mapping,
        labels: kopf.Labels | Mapping,
        meta: kopf.Meta | Mapping,
        name: str,
        namespace: str,
        spec: kopf.Spec | Mapping,
        status: kopf.Status | Mapping,
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
        # Task for when watch is running
        self.task = None

    def __register(self) -> None:
        """
        Add ResourceWatch to cache.
        """
        self.cache_set(CacheTag.WATCH, self.__self_instance_key(), ttl=300)

    def __str__(self) -> str:
        return (
            f"{self.kind} {self.name} ({self.watch_api_version} {self.watch_kind} in {self.watch_namespace})"
            if self.watch_namespace
            else f"{self.kind} {self.name} ({self.watch_api_version} {self.watch_kind})"
        )

    def __self_instance_key(self) -> str:
        return self.__instance_key(
            api_version=self.watch_api_version,
            kind=self.watch_kind,
            namespace=self.watch_namespace,
        )

    @property
    def name_hash(self) -> str:
        return self.name.rsplit("-", 1)[1]

    @property
    def watch_api_version(self) -> str:
        return self.spec["apiVersion"]

    @property
    def watch_kind(self) -> str:
        return self.spec["kind"]

    @property
    def watch_namespace(self) -> str | None:
        return self.spec.get("namespace")

    def __resource_cache_key(self, name: str) -> str:
        """Build unique cache key for a watched resource."""
        return f"{self.name}:{name}"

    async def get_resource(
        self,
        name: str,
        not_found_okay: bool = False,
        use_cache: bool = True,
    ) -> Mapping | None:
        resource_cache_key = self.__resource_cache_key(name)
        if use_cache:
            cached = Cache.get(CacheTag.WATCH_RESOURCE, resource_cache_key)
            if cached:
                return cached
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
            Cache.set(
                CacheTag.WATCH_RESOURCE,
                resource_cache_key,
                resource,
                ttl=Poolboy.resource_refresh_interval,
            )
        return resource

    async def start(self, logger) -> None:
        logger.info(f"Starting {self}")
        self.task = asyncio.create_task(self.watch())

    async def watch(self):
        try:
            if "/" in self.watch_api_version:
                group, version = self.watch_api_version.split("/")
                plural = await poolboy_k8s.kind_to_plural(
                    group=group, version=version, kind=self.watch_kind
                )
                kwargs = {"group": group, "plural": plural, "version": version}
                if self.watch_namespace:
                    method = Poolboy.custom_objects_api.list_namespaced_custom_object
                    kwargs["namespace"] = self.watch_namespace
                else:
                    method = Poolboy.custom_objects_api.list_cluster_custom_object
            elif self.watch_namespace:
                method = getattr(
                    Poolboy.core_v1_api,
                    "list_namespaced_" + inflection.underscore(self.watch_kind),
                )
                kwargs = {"namespace": self.watch_namespace}
            else:
                method = getattr(
                    Poolboy.core_v1_api,
                    "list_" + inflection.underscore(self.watch_kind),
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
                    watch_duration = (
                        datetime.now(timezone.utc) - watch_start_dt
                    ).total_seconds()
                    if watch_duration < 10:
                        await asyncio.sleep(10 - watch_duration)
                except ResourceWatchFailedError as e:
                    logger.warning(f"{self} failed: {e}")
                    watch_duration = (
                        datetime.now(timezone.utc) - watch_start_dt
                    ).total_seconds()
                    if watch_duration < 60:
                        await asyncio.sleep(60 - watch_duration)
                except Exception:
                    logger.exception(f"{self} exception")
                    watch_duration = (
                        datetime.now(timezone.utc) - watch_start_dt
                    ).total_seconds()
                    if watch_duration < 60:
                        await asyncio.sleep(60 - watch_duration)
                logger.debug(f"Restarting {self}")

        except asyncio.CancelledError:
            return

    async def __watch(self, method, **kwargs):
        watch = None
        try:
            watch = kubernetes_asyncio.watch.Watch()
            async for event in watch.stream(method, **kwargs):
                if not isinstance(event, Mapping):
                    raise ResourceWatchFailedError(f"UNKNOWN EVENT: {event}")

                event_obj = event["object"]
                event_type = event["type"]
                if not isinstance(event_obj, Mapping):
                    event_obj = Poolboy.api_client.sanitize_for_serialization(event_obj)
                if event_type == "ERROR":
                    if event_obj["kind"] == "Status":
                        if event_obj["reason"] in ("Expired", "Gone"):
                            raise ResourceWatchRestartError(event_obj["reason"].lower())
                        else:
                            raise ResourceWatchFailedError(
                                f"{event_obj['reason']} {event_obj['message']}"
                            )
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
        event_obj_annotations = event_obj["metadata"].get("annotations")
        if not event_obj_annotations:
            return
        if (
            event_obj_annotations.get(Poolboy.resource_handle_deleted_annotation)
            is not None
        ):
            return

        resource_handle_name = event_obj_annotations.get(
            Poolboy.resource_handle_name_annotation
        )
        resource_index = int(
            event_obj_annotations.get(Poolboy.resource_index_annotation, 0)
        )
        resource_name = event_obj["metadata"]["name"]
        resource_namespace = event_obj["metadata"].get("namespace")
        resource_description = (
            f"{event_obj['apiVersion']} {event_obj['kind']} {resource_name} in {resource_namespace}"
            if resource_namespace
            else f"{event_obj['apiVersion']} {event_obj['kind']} {resource_name}"
        )

        if not resource_handle_name:
            return

        resource_cache_key = self.__resource_cache_key(resource_name)
        if event_type == "DELETED":
            Cache.delete(CacheTag.WATCH_RESOURCE, resource_cache_key)
        else:
            Cache.set(
                CacheTag.WATCH_RESOURCE,
                resource_cache_key,
                event_obj,
                ttl=Poolboy.resource_refresh_interval,
            )

        try:
            resource_handle = await resourcehandle.ResourceHandle.get(
                name=resource_handle_name,
                use_cache=Poolboy.is_standalone,
            )
        except kubernetes_asyncio.client.exceptions.ApiException as exception:
            if exception.status == 404:
                logger.warning(
                    f"ResourceHandle {resource_handle_name} not found for event on {resource_description}"
                )
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
        for idx, resource in enumerate(resource_handle.status_resources):
            if idx == resource_index:
                resource_states.append(event_obj)
                continue
            reference = resource.get("reference")
            if reference:
                if (
                    reference["apiVersion"] == self.watch_api_version
                    and reference["kind"] == self.watch_kind
                    and reference.get("namespace") == self.watch_namespace
                ):
                    resource_states.append(
                        await self.get_resource(
                            name=reference["name"],
                            not_found_okay=True,
                        )
                    )
                else:
                    resource_states.append(
                        await self.get_resource_from_any(
                            api_version=reference["apiVersion"],
                            kind=reference["kind"],
                            name=reference["name"],
                            namespace=reference.get("namespace"),
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
        except kubernetes_asyncio.client.exceptions.ApiException:
            logger.exception(
                f"Failed updating status on {resource_handle} from event on {resource_description}"
            )
