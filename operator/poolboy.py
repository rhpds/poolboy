import os

import kopf
import kubernetes_asyncio


class Poolboy():
    metrics_enabled = os.environ.get('METRICS_ENABLED', 'true').lower() == 'true'
    metrics_port = int(os.environ.get('METRICS_PORT', 9090))
    manage_claims_interval = int(os.environ.get('MANAGE_CLAIMS_INTERVAL', 60))
    manage_handles_interval = int(os.environ.get('MANAGE_HANDLES_INTERVAL', 60))
    manage_pools_interval = int(os.environ.get('MANAGE_POOLS_INTERVAL', 10))

    # Operator mode: 'standalone' or 'distributed'
    # Backward compatibility:
    #   - 'all-in-one' maps to 'standalone'
    #   - 'manager', 'resource-handler', 'resource-watch' map to 'distributed'
    _operator_mode_raw = os.environ.get('OPERATOR_MODE', 'distributed')
    operator_mode = (
        'standalone' if _operator_mode_raw == 'all-in-one'
        else 'distributed' if _operator_mode_raw in ('manager', 'resource-handler', 'resource-watch')
        else _operator_mode_raw
    )
    operator_mode_distributed = operator_mode == 'distributed'
    operator_mode_standalone = operator_mode == 'standalone'

    operator_domain = os.environ.get('OPERATOR_DOMAIN', 'poolboy.gpte.redhat.com')
    operator_version = os.environ.get('OPERATOR_VERSION', 'v1')
    operator_api_version = f"{operator_domain}/{operator_version}"
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
    is_worker = os.environ.get('WORKER', 'false').lower() == 'true'

    # TODO: Remove after all production clusters migrated (used for cleanup only)
    resource_handler_idx_label = f"{operator_domain}/resource-handler-idx"

    # Worker feature flags (loaded from environment)
    # When True, delegate processing to Celery workers
    # When False, process synchronously in the main operator (current behavior)
    workers_error_retry_countdown = int(os.environ.get('WORKERS_ERROR_RETRY_COUNTDOWN', '30'))
    workers_lock_retry_countdown = int(os.environ.get('WORKERS_LOCK_RETRY_COUNTDOWN', '3'))
    workers_resource_pool = os.environ.get('WORKERS_RESOURCE_POOL', 'false').lower() == 'true'
    workers_resource_pool_daemon_mode = os.environ.get('WORKERS_RESOURCE_POOL_DAEMON_MODE', 'scheduler')
    workers_resource_handle = os.environ.get('WORKERS_RESOURCE_HANDLE', 'false').lower() == 'true'
    workers_resource_handle_daemon_mode = os.environ.get('WORKERS_RESOURCE_HANDLE_DAEMON_MODE', 'scheduler')
    workers_resource_claim = os.environ.get('WORKERS_RESOURCE_CLAIM', 'false').lower() == 'true'
    workers_resource_claim_daemon_mode = os.environ.get('WORKERS_RESOURCE_CLAIM_DAEMON_MODE', 'scheduler')
    workers_resource_provider = os.environ.get('WORKERS_RESOURCE_PROVIDER', 'false').lower() == 'true'
    workers_resource_watch = os.environ.get('WORKERS_RESOURCE_WATCH', 'false').lower() == 'true'
    workers_cleanup = os.environ.get('WORKERS_CLEANUP', 'false').lower() == 'true'

    # Redis URL for distributed locking (used by main operator to send tasks)
    redis_url = os.environ.get('REDIS_URL')

    @classmethod
    async def on_cleanup(cls):
        await cls.api_client.close()

    @classmethod
    async def on_startup(cls, logger: kopf.ObjectLogger):
        # Log operator mode on startup
        logger.info(f"Poolboy starting in {cls.operator_mode} mode")
        if cls.operator_mode_distributed:
            logger.info("Distributed mode: delegating to Celery workers")

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
        cls.apps_v1_api = kubernetes_asyncio.client.AppsV1Api(cls.api_client)
        cls.core_v1_api = kubernetes_asyncio.client.CoreV1Api(cls.api_client)
        cls.custom_objects_api = kubernetes_asyncio.client.CustomObjectsApi(cls.api_client)

        # Always run migration cleanup on startup
        # TODO: Remove after all production clusters migrated
        await cls.clear_resource_handler_assignments(logger=logger)

    # TODO: Remove after all production clusters migrated
    @classmethod
    async def clear_resource_handler_assignments(cls, logger: kopf.ObjectLogger):
        """Remove labels and finalizers from legacy manager mode. Keep for migration."""
        handler_finalizer = f"{cls.operator_domain}/handler"
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
                        # Clean both /resource-handler-* AND /handler patterns
                        clean_finalizers = [
                            entry for entry in item['metadata']['finalizers']
                            if not entry.startswith(f"{Poolboy.operator_domain}/resource-handler-")
                            and not entry.startswith(handler_finalizer)  # covers /handler and /handler-N
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
