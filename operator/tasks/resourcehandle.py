"""Celery tasks for ResourceHandle management."""

from celery.utils.log import get_task_logger
from distributed_lock import distributed_lock
from poolboy import Poolboy
from processor.app import WorkerState, app
from scheduler.registry import register_schedule

logger = get_task_logger(__name__)

BATCH_SIZE = 20  # handles per batch - distributes across workers


def _is_transient_exception(exc: Exception) -> bool:
    """Check if exception is transient (expected retry scenario).
    """
    import kubernetes_asyncio

    # Check ApiException first (already imported)
    if isinstance(exc, kubernetes_asyncio.client.exceptions.ApiException):
        return True

    # Check kopf.TemporaryError by class name to avoid importing kopf
    # This works because resourcehandle.py raises kopf.TemporaryError
    exc_class_name = type(exc).__name__
    exc_module = type(exc).__module__
    if exc_class_name == 'TemporaryError' and 'kopf' in exc_module:
        return True

    return False


def _log_and_retry(task, name: str, exc: Exception, action: str):
    """Log exception appropriately and retry the task."""
    countdown = Poolboy.workers_error_retry_countdown

    if _is_transient_exception(exc):
        # Expected transient errors - warning only, no traceback
        logger.warning(f"Handle {name} {action} error: {exc}")
    else:
        # Unexpected error - log with traceback for debugging
        logger.error(f"Handle {name} {action} error: {exc}", exc_info=True)

    raise task.retry(exc=exc, countdown=countdown, max_retries=5)


async def _collect_handles_to_process() -> list:
    """Collect all handles that need processing (not recently processed)."""
    handles_to_process = []
    _continue = None

    while True:
        handle_list = await Poolboy.custom_objects_api.list_namespaced_custom_object(
            group=Poolboy.operator_domain,
            namespace=Poolboy.namespace,
            plural='resourcehandles',
            version=Poolboy.operator_version,
            _continue=_continue,
            limit=50,
        )

        for item in handle_list.get('items', []):
            # Skip ignored handles
            if Poolboy.ignore_label in item['metadata'].get('labels', {}):
                continue

            handles_to_process.append(item)

        _continue = handle_list['metadata'].get('continue')
        if not _continue:
            break

    return handles_to_process


async def _delete_handle(definition: dict) -> dict:
    """Async wrapper for ResourceHandle.handle_delete().
    
    Note: We do NOT refetch for delete operations. The handle may already
    be deleted from K8s, but we still need to propagate the delete to
    child resources (ResourceClaimTest, etc.) using the original definition.
    """
    import resourcehandle
    handle = resourcehandle.ResourceHandle.from_definition(definition)
    await handle.handle_delete(logger=logger)
    return {"status": "completed", "handle": handle.name}


def _dispatch_batch(handles: list) -> int:
    """Dispatch a batch of handles as individual tasks."""
    dispatched = 0
    for item in handles:
        uid = item['metadata']['uid']
        rv = item['metadata']['resourceVersion']
        kwargs = {
            'definition': item,
            'name': item['metadata']['name'],
            'namespace': item['metadata']['namespace'],
        }
        manage_handle.apply_async(kwargs=kwargs, task_id=f"handle-{uid}-{rv}")
        dispatched += 1
    return dispatched


async def _manage_handle(definition: dict) -> dict:
    """Async wrapper for ResourceHandle.manage()."""
    import resourcehandle
    handle = resourcehandle.ResourceHandle.from_definition(definition)
    # Refetch to get current state from K8s API (avoid stale data)
    handle = await handle.refetch()
    if not handle:
        # Handle was deleted between dispatch and execution
        return {"status": "skipped", "reason": "not_found", "handle": definition['metadata']['name']}
    await handle.manage(logger=logger)
    return {"status": "completed", "handle": handle.name}


@app.task(bind=True, acks_late=True)
def delete_handle(self, definition: dict, name: str, namespace: str):
    """Execute ResourceHandle.handle_delete() in a worker."""
    uid = definition['metadata']['uid']
    lock_key = f"resource_handle:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Handle {name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            return WorkerState.run_async(_delete_handle(definition))
        except Exception as e:
            _log_and_retry(self, name, e, "delete")


def dispatch_delete_handle(definition: dict, name: str, namespace: str):
    """Dispatch delete_handle task with unique task_id."""
    uid = definition['metadata']['uid']
    rv = definition['metadata']['resourceVersion']
    kwargs = {'definition': definition, 'name': name, 'namespace': namespace}
    delete_handle.apply_async(
        kwargs=kwargs,
        task_id=f"handle-delete-{uid}-{rv}",
    )


def dispatch_manage_handle(definition: dict, name: str, namespace: str):
    """Dispatch manage_handle task. Always dispatches for operator events."""
    uid = definition['metadata']['uid']
    rv = definition['metadata']['resourceVersion']
    kwargs = {'definition': definition, 'name': name, 'namespace': namespace}
    manage_handle.apply_async(
        kwargs=kwargs,
        task_id=f"handle-{uid}-{rv}",
    )


@app.task(bind=True, acks_late=True)
def manage_handle(self, definition: dict, name: str, namespace: str):
    """Execute ResourceHandle.manage() in a worker."""
    uid = definition['metadata']['uid']
    lock_key = f"resource_handle:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Handle {name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            return WorkerState.run_async(_manage_handle(definition))
        except Exception as e:
            _log_and_retry(self, name, e, "manage")


@register_schedule(
    task_name="maintain-all-handles",
    seconds=60,
    description="Periodic task to reconcile all ResourceHandles",
    owner="poolboy",
)
@app.task(name="tasks.resourcehandle.maintain_all_handles")
def maintain_all_handles():
    """Periodic task for Celery Beat - reconcile all handles using group for distribution."""
    from celery import group

    lock_key = "maintain_all_handles:global"

    with distributed_lock(lock_key, timeout=300) as acquired:
        if not acquired:
            return {"status": "skipped", "reason": "already_running"}

        # Collect all handles that need processing
        handles = WorkerState.run_async(_collect_handles_to_process())

        if not handles:
            return {"status": "completed", "total": 0, "batches": 0}

        # Split into batches and dispatch using group (distributes across workers)
        batches = [handles[i:i + BATCH_SIZE] for i in range(0, len(handles), BATCH_SIZE)]

        # Create group of batch tasks - Celery will distribute across available workers
        batch_group = group(process_handle_batch.s(batch) for batch in batches)
        batch_group.apply_async()

        logger.info(f"Handle maintenance: {len(handles)} handles in {len(batches)} batches")
        return {"status": "dispatched", "total": len(handles), "batches": len(batches)}


@app.task(bind=True)
def process_handle_batch(self, handles: list):
    """Process a batch of handles. Each batch runs on a different worker."""
    return _dispatch_batch(handles)
