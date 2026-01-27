"""Celery tasks for ResourceClaim management."""

from celery.utils.log import get_task_logger
from distributed_lock import distributed_lock
from poolboy import Poolboy
from processor.app import WorkerState, app
from scheduler.registry import register_schedule

logger = get_task_logger(__name__)

BATCH_SIZE = 20  # claims per batch - distributes across workers


def _is_transient_exception(exc: Exception) -> bool:
    """Check if exception is transient (expected retry scenario)."""
    import kubernetes_asyncio

    if isinstance(exc, kubernetes_asyncio.client.exceptions.ApiException):
        return True

    exc_class_name = type(exc).__name__
    exc_module = type(exc).__module__
    if exc_class_name == "TemporaryError" and "kopf" in exc_module:
        return True

    return False


def _log_and_retry(task, name: str, namespace: str, exc: Exception, action: str):
    """Log exception appropriately and retry the task."""
    countdown = Poolboy.workers_error_retry_countdown

    if _is_transient_exception(exc):
        logger.warning(f"Claim {namespace}/{name} {action} error: {exc}")
    else:
        logger.error(f"Claim {namespace}/{name} {action} error: {exc}", exc_info=True)

    raise task.retry(exc=exc, countdown=countdown, max_retries=5)


async def _collect_claims_to_process() -> list:
    """Collect all claims that need processing (not recently processed)."""
    claims_to_process = []
    _continue = None

    while True:
        # Note: Using cluster-wide listing since claims exist in user namespaces
        claim_list = await Poolboy.custom_objects_api.list_cluster_custom_object(
            group=Poolboy.operator_domain,
            plural="resourceclaims",
            version=Poolboy.operator_version,
            _continue=_continue,
            limit=50,
        )

        for item in claim_list.get("items", []):
            # Skip ignored claims
            if Poolboy.ignore_label in item["metadata"].get("labels", {}):
                continue

            claims_to_process.append(item)

        _continue = claim_list["metadata"].get("continue")
        if not _continue:
            break

    return claims_to_process


async def _delete_claim(definition: dict) -> dict:
    """Async wrapper for ResourceClaim.handle_delete().

    Note: We do NOT refetch for delete operations. The claim may already
    be deleted from K8s, but we still need to propagate the delete to
    the ResourceHandle using the original definition.
    """
    import resourceclaim

    claim = resourceclaim.ResourceClaim.from_definition(definition)
    await claim.handle_delete(logger=logger)
    await claim.unregister(name=claim.name, namespace=claim.namespace)
    return {"status": "completed", "claim": claim.name, "namespace": claim.namespace}


def _dispatch_batch(claims: list) -> int:
    """Dispatch a batch of claims as individual tasks.

    Note: Uses timestamp (truncated to minute) instead of resourceVersion for task_id.
    This allows periodic reprocessing even when resourceVersion hasn't changed,
    which is necessary for time-based triggers like lifespan.start.
    """
    import time

    ts_minute = int(time.time() // 60)  # One dispatch allowed per minute per claim

    dispatched = 0
    for item in claims:
        uid = item["metadata"]["uid"]
        kwargs = {
            "definition": item,
            "name": item["metadata"]["name"],
            "namespace": item["metadata"]["namespace"],
        }
        # Use timestamp instead of resourceVersion to allow periodic reprocessing
        manage_claim.apply_async(
            kwargs=kwargs, task_id=f"claim-sched-{uid}-{ts_minute}"
        )
        dispatched += 1
    return dispatched


async def _manage_claim(definition: dict) -> dict:
    """Async wrapper for ResourceClaim.manage()."""
    import resourceclaim

    claim = resourceclaim.ResourceClaim.from_definition(definition)
    # Refetch to get current state from K8s API (avoid stale data)
    claim = await claim.refetch()
    if not claim:
        # Claim was deleted between dispatch and execution
        return {
            "status": "skipped",
            "reason": "not_found",
            "claim": definition["metadata"]["name"],
        }

    # Register claim in cache to keep it fresh
    await claim.register_definition(claim.definition)

    await claim.manage(logger=logger)
    return {"status": "completed", "claim": claim.name, "namespace": claim.namespace}


@app.task(bind=True, acks_late=True)
def delete_claim(self, definition: dict, name: str, namespace: str):
    """Execute ResourceClaim.handle_delete() in a worker."""
    uid = definition["metadata"]["uid"]
    lock_key = f"resource_claim:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Claim {namespace}/{name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            return WorkerState.run_async(_delete_claim(definition))
        except Exception as e:
            _log_and_retry(self, name, namespace, e, "delete")


def dispatch_delete_claim(definition: dict, name: str, namespace: str):
    """Dispatch delete_claim task with unique task_id."""
    uid = definition["metadata"]["uid"]
    rv = definition["metadata"]["resourceVersion"]
    kwargs = {"definition": definition, "name": name, "namespace": namespace}
    delete_claim.apply_async(
        kwargs=kwargs,
        task_id=f"claim-delete-{uid}-{rv}",
    )


def dispatch_manage_claim(definition: dict, name: str, namespace: str):
    """Dispatch manage_claim task. Always dispatches for operator events."""
    uid = definition["metadata"]["uid"]
    rv = definition["metadata"]["resourceVersion"]
    kwargs = {"definition": definition, "name": name, "namespace": namespace}
    manage_claim.apply_async(
        kwargs=kwargs,
        task_id=f"claim-{uid}-{rv}",
    )


@app.task(bind=True, acks_late=True)
def manage_claim(self, definition: dict, name: str, namespace: str):
    """Execute ResourceClaim.manage() in a worker."""
    uid = definition["metadata"]["uid"]
    lock_key = f"resource_claim:{uid}"

    with distributed_lock(lock_key, timeout=60) as acquired:
        if not acquired:
            logger.debug(f"Claim {namespace}/{name} locked, retrying")
            countdown = Poolboy.workers_lock_retry_countdown
            raise self.retry(countdown=countdown, max_retries=None)

        try:
            return WorkerState.run_async(_manage_claim(definition))
        except Exception as e:
            _log_and_retry(self, name, namespace, e, "manage")


@register_schedule(
    task_name="maintain-all-claims",
    seconds=60,
    description="Periodic task to reconcile all ResourceClaims",
    owner="poolboy",
)
@app.task(name="tasks.resourceclaim.maintain_all_claims")
def maintain_all_claims():
    """Periodic task for Celery Beat - reconcile all claims using group for distribution."""
    from celery import group

    lock_key = "maintain_all_claims:global"

    with distributed_lock(lock_key, timeout=300) as acquired:
        if not acquired:
            return {"status": "skipped", "reason": "already_running"}

        # Collect all claims that need processing
        claims = WorkerState.run_async(_collect_claims_to_process())

        if not claims:
            return {"status": "completed", "total": 0, "batches": 0}

        # Split into batches and dispatch using group (distributes across workers)
        batches = [
            claims[i : i + BATCH_SIZE] for i in range(0, len(claims), BATCH_SIZE)
        ]

        # Create group of batch tasks - Celery will distribute across available workers
        batch_group = group(process_claim_batch.s(batch) for batch in batches)
        batch_group.apply_async()

        logger.info(
            f"Claim maintenance: {len(claims)} claims in {len(batches)} batches"
        )
        return {"status": "dispatched", "total": len(claims), "batches": len(batches)}


@app.task(bind=True)
def process_claim_batch(self, claims: list):
    """Process a batch of claims. Each batch runs on a different worker."""
    return _dispatch_batch(claims)
