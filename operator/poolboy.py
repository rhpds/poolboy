import os

from copy import deepcopy
from uuid import UUID

import kopf
import kubernetes_asyncio

class Poolboy():
    manage_claims_interval = int(os.environ.get('MANAGE_CLAIMS_INTERVAL', 60))
    manage_handles_interval = int(os.environ.get('MANAGE_HANDLES_INTERVAL', 60))
    manage_pools_interval = int(os.environ.get('MANAGE_POOLS_INTERVAL', 10))
    operator_mode = os.environ.get('OPERATOR_MODE', 'all-in-one')
    operator_mode_all_in_one = operator_mode == 'all-in-one'
    operator_mode_manager = operator_mode == 'manager'
    operator_mode_resource_handler = operator_mode == 'resource-handler'
    operator_mode_resource_watch = operator_mode == 'resource-watch'
    operator_domain = os.environ.get('OPERATOR_DOMAIN', 'poolboy.gpte.redhat.com')
    operator_version = os.environ.get('OPERATOR_VERSION', 'v1')
    operator_api_version = f"{operator_domain}/{operator_version}"
    resource_watch_name = os.environ.get('WATCH_NAME')
    resource_handler_count = int(os.environ.get('RESOURCE_HANDLER_COUNT', 1))
    resource_handler_idx = int(os.environ.get('RESOURCE_HANDLER_IDX', 0))
    resource_refresh_interval = int(os.environ.get('RESOURCE_REFRESH_INTERVAL', 600))
    resource_handle_deleted_annotation = f"{operator_domain}/resource-handle-deleted"
    resource_claim_name_annotation = f"{operator_domain}/resource-claim-name"
    resource_claim_name_label = f"{operator_domain}/resource-claim-name"
    resource_claim_namespace_annotation = f"{operator_domain}/resource-claim-namespace"
    resource_claim_namespace_label = f"{operator_domain}/resource-claim-namespace"
    resource_handle_name_annotation = f"{operator_domain}/resource-handle-name"
    resource_handle_namespace_annotation = f"{operator_domain}/resource-handle-namespace"
    resource_handle_uid_annotation = f"{operator_domain}/resource-handle-uid"
    resource_index_annotation = f"{operator_domain}/resource-index"
    resource_pool_name_annotation = f"{operator_domain}/resource-pool-name"
    resource_pool_name_label = f"{operator_domain}/resource-pool-name"
    resource_pool_namespace_annotation = f"{operator_domain}/resource-pool-namespace"
    resource_pool_namespace_label = f"{operator_domain}/resource-pool-namespace"
    resource_provider_name_annotation = f"{operator_domain}/resource-provider-name"
    resource_provider_namespace_annotation = f"{operator_domain}/resource-provider-namespace"
    resource_requester_email_annotation = f"{operator_domain}/resource-requester-email"
    resource_requester_name_annotation = f"{operator_domain}/resource-requester-name"
    resource_requester_user_annotation = f"{operator_domain}/resource-requester-user"
    resource_requester_preferred_username_annotation = f"{operator_domain}/resource-requester-preferred-username"
    ignore_label = f"{operator_domain}/ignore"
    resource_handler_idx_label = f"{operator_domain}/resource-handler-idx"

    @classmethod
    async def on_cleanup(cls):
        await cls.api_client.close()

    @classmethod
    async def on_startup(cls, logger: kopf.ObjectLogger):
        if os.path.exists('/run/secrets/kubernetes.io/serviceaccount'):
            kubernetes_asyncio.config.load_incluster_config()
            with open('/run/secrets/kubernetes.io/serviceaccount/namespace', encoding='utf-8') as f:
                cls.namespace = f.read()
        else:
            await kubernetes_asyncio.config.load_kube_config()
            if 'OPERATOR_NAMESPACE' in os.environ:
                cls.namespace = os.environ['OPERATOR_NAMESPACE']
            else:
                raise Exception(
                    'Unable to determine operator namespace. '
                    'Please set OPERATOR_NAMESPACE environment variable.'
                )

        cls.api_client = kubernetes_asyncio.client.ApiClient()
        cls.core_v1_api = kubernetes_asyncio.client.CoreV1Api(cls.api_client)
        cls.custom_objects_api = kubernetes_asyncio.client.CustomObjectsApi(cls.api_client)

        if cls.operator_mode == 'manager':
            await cls.assign_resource_handlers(logger=logger)
            await cls.start_resource_handlers(logger=logger)
        elif cls.operator_mode == 'all-in-one':
            await cls.clear_resource_handler_assignments(logger=logger)

    @classmethod
    async def assign_resource_handlers(cls, logger: kopf.ObjectLogger):
        """Label ResourceHandles and ResourcePools to match to appropriate handlers.
        Clear any extraneous finalizers."""
        for plural in ('resourcehandles', 'resourcepools'):
            _continue = None
            while True:
                obj_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
                    group=Poolboy.operator_domain,
                    namespace=Poolboy.namespace,
                    plural=plural,
                    version=Poolboy.operator_version,
                    _continue = _continue,
                    limit = 50,
                )
                for item in obj_list.get('items', []):
                    kind = item['kind']
                    name = item['metadata']['name']
                    patch = []
                    resource_handler_idx = int(UUID(item['metadata']['uid'])) % cls.resource_handler_count
                    if resource_handler_idx != int(item['metadata'].get('labels', {}).get(cls.resource_handler_idx_label, '-1')):
                        if 'labels' in item['metadata']:
                            patch.append({
                                "op": "add",
                                "path": "/metadata/labels",
                                "value": {
                                    cls.resource_handler_idx_label: str(resource_handler_idx)
                                }
                            })
                        else:
                            patch.append({
                                "op": "add",
                                "path": f"/metadata/labels/{cls.resource_handler_idx_label.replace('/', '~1')}",
                                "value": str(resource_handler_idx),
                            })
                    if 'finalizers' in item['metadata']:
                        clean_finalizers = [
                            entry for entry in item['metadata']['finalizers']
                            if entry == f"{Poolboy.operator_domain}/resource-handler-{resource_handler_idx}"
                            or not entry.startswith(f"{Poolboy.operator_domain}/resource-handler-")
                        ]
                        if clean_finalizers != item['metadata']['finalizers']:
                            patch.append({
                                "op": "replace",
                                "path": "/metadata/finalizers",
                                "value": clean_finalizers,
                            })
                    if patch:
                        logger.info(
                            f"Patching {kind} {name} to assign resource handler"
                        )
                        try:
                            await Poolboy.custom_objects_api.patch_namespaced_custom_object(
                                group=Poolboy.operator_domain,
                                name=item['metadata']['name'],
                                namespace=Poolboy.namespace,
                                plural=plural,
                                version=Poolboy.operator_version,
                                body=patch,
                                _content_type = 'application/json-patch+json',
                            )
                        except:
                            logger.exception("Patch failed.")

                _continue = obj_list['metadata'].get('continue')
                if not _continue:
                    break

    @classmethod
    async def clear_resource_handler_assignments(cls, logger: kopf.ObjectLogger):
        """Remove labels and finalizers applied to run in manager mode."""
        for plural in ('resourcehandles', 'resourcepools'):
            _continue = None
            while True:
                obj_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
                    group=Poolboy.operator_domain,
                    namespace=Poolboy.namespace,
                    plural=plural,
                    version=Poolboy.operator_version,
                    _continue = _continue,
                    limit = 50,
                )
                for item in obj_list.get('items', []):
                    kind = item['kind']
                    name = item['metadata']['name']
                    patch = []
                    if cls.resource_handler_idx_label in item['metadata'].get('labels', {}):
                        patch.append({
                            "op": "remove",
                            "path": f"/metadata/labels/{cls.resource_handler_idx_label.replace('/', '~1')}",
                        })
                    if 'finalizers' in item['metadata']:
                        clean_finalizers = [
                            entry for entry in item['metadata']['finalizers']
                            if not entry.startswith(f"{Poolboy.operator_domain}/resource-handler-")
                        ]
                        if clean_finalizers != item['metadata']['finalizers']:
                            patch.append({
                                "op": "replace",
                                "path": "/metadata/finalizers",
                                "value": clean_finalizers,
                            })
                    if patch:
                        logger.info(
                            f"Patching {kind} {name} to clear resource handler assignment"
                        )
                        try:
                            await Poolboy.custom_objects_api.patch_namespaced_custom_object(
                                group=Poolboy.operator_domain,
                                name=item['metadata']['name'],
                                namespace=Poolboy.namespace,
                                plural=plural,
                                version=Poolboy.operator_version,
                                body=patch,
                                _content_type = 'application/json-patch+json',
                            )
                        except:
                            logger.exception("Patch failed.")

                _continue = obj_list['metadata'].get('continue')
                if not _continue:
                    break

    @classmethod
    async def start_resource_handlers(cls, logger: kopf.ObjectLogger):
        cls.manager_pod = await cls.core_v1_api.read_namespaced_pod(
            name=os.environ['HOSTNAME'],
            namespace=cls.namespace,
        )
        logger.info(f"Manager running in pod {cls.manager_pod.metadata.name}")
        for idx in range(Poolboy.resource_handler_count):
            pod = kubernetes_asyncio.client.V1Pod(
                api_version="v1",
                kind="Pod",
                metadata=kubernetes_asyncio.client.V1ObjectMeta(
                    name=f"{cls.manager_pod.metadata.name}-handler-{idx}",
                    namespace=cls.namespace,
                    owner_references=[
                        kubernetes_asyncio.client.V1OwnerReference(
                            api_version=cls.manager_pod.api_version,
                            controller=True,
                            kind=cls.manager_pod.kind,
                            name=cls.manager_pod.metadata.name,
                            uid=cls.manager_pod.metadata.uid,
                        )
                    ]
                ),
                spec=deepcopy(cls.manager_pod.spec),
            )
            pod.spec.containers[0].env = [
                env_var
                for env_var in pod.spec.containers[0].env
                if env_var.name != 'OPERATOR_MODE'
            ]
            pod.spec.containers[0].env.append(
                kubernetes_asyncio.client.V1EnvVar(
                    name='OPERATOR_MODE',
                    value='resource-handler',
                )
            )
            pod.spec.containers[0].env.append(
                kubernetes_asyncio.client.V1EnvVar(
                    name='RESOURCE_HANDLER_IDX',
                    value=str(idx),
                )
            )
            pod.spec.nodeName = None
            await cls.core_v1_api.create_namespaced_pod(
                namespace=cls.namespace,
                body=pod,
            )
