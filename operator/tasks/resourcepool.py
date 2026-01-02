"""Celery tasks for ResourcePool management."""

from celery.utils.log import get_task_logger
from distributed_lock import distributed_lock
from processor.app import WorkerState, app
from scheduler.registry import register_schedule

logger = get_task_logger(__name__)


async def _delete_pool_handles(definition: dict) -> dict:
    """Async wrapper for ResourcePool.handle_delete()."""
    import resourcepool
    pool = resourcepool.ResourcePool.from_definition(definition)
    await pool.handle_delete(logger=logger)
    return {"status": "completed", "pool": pool.name}


async def _maintain_all_pools() -> dict:
    """List all pools and dispatch manage_pool for each unprocessed."""
    from poolboy import Poolboy

    pool_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
        group=Poolboy.operator_domain,
        namespace=Poolboy.namespace,
        plural='resourcepools',
        version=Poolboy.operator_version,
    )

    dispatched = 0
    for item in pool_list.get('items', []):
        uid = item['metadata']['uid']
        rv = item['metadata']['resourceVersion']
        kwargs = {
            'definition': item,
            'name': item['metadata']['name'],
            'namespace': item['metadata']['namespace'],
        }
        manage_pool.apply_async(kwargs=kwargs, task_id=f"pool-{uid}-{rv}")
        dispatched += 1

    return {"dispatched": dispatched}


async def _manage_pool(definition: dict) -> dict:
    """Async wrapper for ResourcePool.manage()."""
    import resourcepool
    pool = resourcepool.ResourcePool.from_definition(definition)
    await pool.manage(logger=logger)
    return {"status": "completed", "pool": pool.name}


@app.task(bind=True, acks_late=True)
def delete_pool_handles(self, definition: dict, name: str, namespace: str):
    """Execute ResourcePool.handle_delete() in a worker."""
    from poolboy import Poolboy

    uid = definition['metadata']['uid']
    lock_key = f"resource_pool:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Pool {namespace}/{name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            return WorkerState.run_async(_delete_pool_handles(definition))
        except Exception as e:
            logger.error(f"Pool {namespace}/{name} delete error: {e}")
            raise self.retry(exc=e, countdown=Poolboy.workers_error_retry_countdown, max_retries=5)


def dispatch_delete_pool_handles(definition: dict, name: str, namespace: str):
    """Dispatch delete_pool_handles task with unique task_id."""
    uid = definition['metadata']['uid']
    rv = definition['metadata']['resourceVersion']
    kwargs = {'definition': definition, 'name': name, 'namespace': namespace}
    delete_pool_handles.apply_async(
        kwargs=kwargs,
        task_id=f"pool-delete-{uid}-{rv}",
    )


def dispatch_manage_pool(definition: dict, name: str, namespace: str):
    """Dispatch manage_pool task. Always dispatches for operator events."""
    uid = definition['metadata']['uid']
    rv = definition['metadata']['resourceVersion']
    kwargs = {'definition': definition, 'name': name, 'namespace': namespace}
    manage_pool.apply_async(
        kwargs=kwargs,
        task_id=f"pool-{uid}-{rv}",
    )


@register_schedule(
    task_name="maintain-all-pools",
    seconds=30,
    description="Periodic task to reconcile all ResourcePools",
    owner="poolboy",
)
@app.task(name="tasks.resourcepool.maintain_all_pools")
def maintain_all_pools():
    """Periodic task for Celery Beat - reconcile all pools."""
    lock_key = "maintain_all_pools:global"

    with distributed_lock(lock_key, timeout=300) as acquired:
        if not acquired:
            return {"status": "skipped", "reason": "already_running"}

        result = WorkerState.run_async(_maintain_all_pools())
        if result.get("dispatched", 0) > 0:
            logger.info(f"Maintenance dispatched: {result['dispatched']}")
        return result


@app.task(bind=True, acks_late=True)
def manage_pool(self, definition: dict, name: str, namespace: str):
    """Execute ResourcePool.manage() in a worker."""
    from poolboy import Poolboy

    uid = definition['metadata']['uid']
    lock_key = f"resource_pool:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Pool {namespace}/{name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            result = WorkerState.run_async(_manage_pool(definition))
            return result
        except Exception as e:
            logger.error(f"Pool {namespace}/{name} error: {e}")
            raise self.retry(exc=e, countdown=Poolboy.workers_error_retry_countdown, max_retries=5)
