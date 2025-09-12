from typing import List, Mapping

import inflection
import kopf
import kubernetes_asyncio
from metrics.timer_decorator import async_timer
from poolboy import Poolboy

api_groups = {}

class KindNotFoundException(Exception):
    pass

@async_timer(app="PoolboyK8s")
async def create_object(definition: Mapping) -> Mapping:
    if '/' in definition['apiVersion']:
        return await create_custom_object(definition)
    else:
        return await create_core_object(definition)

@async_timer(app="PoolboyK8s")
async def create_core_object(definition: Mapping) -> Mapping:
    if 'namespace' in definition['metadata']:
        return await create_namespaced_core_object(definition)
    else:
        return await create_cluster_core_object(definition)

@async_timer(app="PoolboyK8s")
async def create_custom_object(definition: Mapping) -> Mapping:
    group, version = definition['apiVersion'].split('/')
    plural = await kind_to_plural(group=group, kind=definition['kind'], version=version)
    namespace = definition['metadata'].get('namespace')
    if namespace:
        return await Poolboy.custom_objects_api.create_namespaced_custom_object(
            body = definition,
            group = group,
            namespace = namespace,
            plural = plural,
            version = version,
        )
    else:
        return await Poolboy.custom_objects_api.create_cluster_custom_object(
            body = definition,
            group = group,
            plural = plural,
            version = version,
        )

@async_timer(app="PoolboyK8s")
async def create_cluster_core_object(definition: Mapping) -> Mapping:
    kind = definition['kind']
    method = getattr(
        Poolboy.core_v1_api,
        'create_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(body=definition)
    )

@async_timer(app="PoolboyK8s")
async def create_namespaced_core_object(definition: Mapping) -> Mapping:
    kind = definition['kind']
    namespace = definition['metadata']['namespace']
    method = getattr(
        Poolboy.core_v1_api,
        'create_namespaced_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(body=definition, namespace=namespace)
    )

@async_timer(app="PoolboyK8s")
async def delete_core_object(
    kind: str,
    name: str,
    namespace: str|None = None,
) -> Mapping:
    if namespace:
        return await delete_namespaced_core_object(
            kind = kind,
            name = name,
            namespace = namespace,
        )
    else:
        return await delete_cluster_core_object(
            kind = kind,
            name = name,
        )

@async_timer(app="PoolboyK8s")
async def delete_cluster_core_object(
    kind: str,
    name: str,
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'delete_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(name=name)
    )

@async_timer(app="PoolboyK8s")
async def delete_namespaced_core_object(
    kind: str,
    name: str,
    namespace: str,
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'delete_namespaced_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(name=name, namespace=namespace)
    )

@async_timer(app="PoolboyK8s")
async def delete_custom_object(
    group: str,
    version: str,
    kind: str,
    name: str,
    namespace: str|None = None,
) -> Mapping|None:
    plural = await kind_to_plural(group=group, kind=kind, version=version)
    if namespace:
        return await Poolboy.custom_objects_api.delete_namespaced_custom_object(
            group = group,
            name = name,
            namespace = namespace,
            plural = plural,
            version = version,
        )
    else:
        return await Poolboy.custom_objects_api.delete_cluster_custom_object(
            group = group,
            name = name,
            plural = plural,
            version = version,
        )

@async_timer(app="PoolboyK8s")
async def delete_object(
    api_version: str,
    kind: str,
    name: str,
    namespace: str = None,
) -> Mapping|None:
    if '/' in api_version:
        group, version = api_version.split('/')
        return await delete_custom_object(
            group = group,
            kind = kind,
            name = name,
            namespace = namespace,
            version = version,
        )
    else:
        return await delete_core_object(
            kind = kind,
            name = name,
            namespace = namespace
        )

@async_timer(app="PoolboyK8s")
async def get_object(
    api_version: str,
    kind: str,
    name: str,
    namespace: str|None = None,
) -> Mapping|None:
    if '/' in api_version:
        group, version = api_version.split('/')
        return await get_custom_object(
            group = group,
            kind = kind,
            name = name,
            namespace = namespace,
            version = version,
        )
    else:
        return await get_core_object(
            kind = kind,
            name = name,
            namespace = namespace
        )

@async_timer(app="PoolboyK8s")
async def get_core_object(
    kind: str,
    name: str,
    namespace: str|None = None,
) -> Mapping:
    if namespace:
        return await get_namespaced_core_object(
            kind = kind,
            name = name,
            namespace = namespace,
        )
    else:
        return await get_cluster_core_object(
            kind = kind,
            name = name,
        )

@async_timer(app="PoolboyK8s")
async def get_cluster_core_object(
    kind: str,
    name: str,
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'read_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(name=name)
    )

@async_timer(app="PoolboyK8s")
async def get_namespaced_core_object(
    kind: str,
    name: str,
    namespace: str,
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'read_namespaced_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(name=name, namespace=namespace)
    )

@async_timer(app="PoolboyK8s")
async def get_custom_object(
    group: str,
    version: str,
    kind: str,
    name: str,
    namespace: str|None = None,
) -> Mapping|None:
    plural = await kind_to_plural(group=group, kind=kind, version=version)
    if namespace:
        return await Poolboy.custom_objects_api.get_namespaced_custom_object(
            group = group,
            name = name,
            namespace = namespace,
            plural = plural,
            version = version,
        )
    else:
        return await Poolboy.custom_objects_api.get_cluster_custom_object(
            group = group,
            name = name,
            plural = plural,
            version = version,
        )

@async_timer(app="PoolboyK8s")
async def get_requester_from_namespace(namespace: str) -> tuple[Mapping|None, List[Mapping]|None]:
    try:
        namespace_obj = await Poolboy.core_v1_api.read_namespace(namespace)
    except kubernetes_asyncio.client.exceptions.ApiException as e:
        if e.status == 404:
            return None, []
        else:
            raise

    user_name = namespace_obj.metadata.annotations.get('openshift.io/requester')
    if not user_name:
        return None, []

    try:
        user = await Poolboy.custom_objects_api.get_cluster_custom_object(
            'user.openshift.io', 'v1', 'users', user_name
        )
    except kubernetes_asyncio.client.exceptions.ApiException as e:
        if e.status == 404:
            user = {
                "metadata": {
                    "name": user_name,
                },
                "identities": []
            }
            return user, []
        else:
            raise

    identities = []
    for identity_name in user.get('identities', []):
        try:
            identity = await Poolboy.custom_objects_api.get_cluster_custom_object(
                'user.openshift.io', 'v1', 'identities', identity_name
            )
            identities.append(identity)
        except kubernetes_asyncio.client.exceptions.ApiException as e:
            if e.status == 404:
                identities.append({
                    "metadata": {
                        "name": identity_name,
                    },
                    "extra": {},
                })
            else:
                raise

    return user, identities

@async_timer(app="PoolboyK8s")
async def kind_to_plural(
    group: str,
    kind: str,
    version: str,
) -> str:
    api_group = api_groups.get(group)
    if api_group:
        api_group_version  = api_group.get(version)
        if api_group_version:
            for resource in api_group_version['resources']:
                if resource['kind'] == kind:
                    return resource['name']

    try:
        resp = await Poolboy.api_client.call_api(
            method = 'GET',
            resource_path = f"/apis/{group}/{version}",
            auth_settings=['BearerToken'],
            response_types_map = {
                200: "object",
            }
        )
    except kubernetes_asyncio.client.exceptions.ApiException as e:
        if e.status == 404:
            raise kopf.TemporaryError(
                f"API {group}/{version} not found",
                delay=600
            )
        else:
            raise

    api_group_version = resp[0]
    if group not in api_groups:
        api_groups[group] = {}
    api_groups[group][version] = api_group_version

    for resource in api_group_version['resources']:
        if resource['kind'] == kind:
            return resource['name']

    raise kopf.TemporaryError(
        f"API {group}/version does not have kind {kind}",
        delay=600
    )

@async_timer(app="PoolboyK8s")
async def patch_core_object(
    kind: str,
    name: str,
    namespace: str,
    patch: List[Mapping],
) -> Mapping:
    if namespace:
        return await patch_namespaced_core_object(
            kind = kind,
            name = name,
            namespace = namespace,
            patch = patch,
        )
    else:
        return await patch_cluster_core_object(
            kind = kind,
            name = name,
            patch = patch,
        )

@async_timer(app="PoolboyK8s")
async def patch_cluster_core_object(
    kind: str,
    name: str,
    patch: List[Mapping],
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'patch_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(
            name = name,
            body = patch,
            _content_type = 'application/json-patch+json',
        )
    )

@async_timer(app="PoolboyK8s")
async def patch_namespaced_core_object(
    kind: str,
    name: str,
    namespace: str,
    patch: List[Mapping],
) -> Mapping:
    method = getattr(
        Poolboy.core_v1_api,
        'patch_namespaced_' + inflection.underscore(kind)
    )
    return Poolboy.api_client.sanitize_for_serialization(
        await method(
            name = name,
            namespace = namespace,
            body = patch,
            _content_type = 'application/json-patch+json',
        )
    )

@async_timer(app="PoolboyK8s")
async def patch_custom_object(
    group: str,
    kind: str,
    name: str,
    namespace: str,
    patch: List[Mapping],
    version: str,
) -> Mapping:
    plural = await kind_to_plural(group=group, kind=kind, version=version)
    if namespace:
        return await Poolboy.custom_objects_api.patch_namespaced_custom_object(
            group = group,
            name = name,
            namespace = namespace,
            plural = plural,
            version = version,
            body = patch,
            _content_type = 'application/json-patch+json',
        )
    else:
        return await Poolboy.custom_objects_api.patch_cluster_custom_object(
            group = group,
            name = name,
            plural = plural,
            version = version,
            body = patch,
            _content_type = 'application/json-patch+json',
        )

@async_timer(app="PoolboyK8s")
async def patch_object(
    api_version: str,
    kind: str,
    name: str,
    namespace: str,
    patch: List[Mapping],
) -> List:
    if '/' in api_version:
        group, version = api_version.split('/')
        return await patch_custom_object(
            group = group,
            kind = kind,
            name = name,
            namespace = namespace,
            patch = patch,
            version = version,
        )
    else:
        return await patch_core_object(
            kind = kind,
            name = name,
            namespace = namespace,
            patch = patch,
        )
