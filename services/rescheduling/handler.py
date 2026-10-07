import asyncio
import logging
import time
import uuid

import grpc
from fastapi import HTTPException

from . import audit
from .auth import can_reschedule
from .kafka_publisher import publish_event
from .models import BatchRequest, Job

log = logging.getLogger(__name__)

MAX_CONCURRENCY = 10
_IN_FLIGHT = object()
# ponytail: in-process store, lost on restart and not shared across replicas.
# Move to a Postgres table with UNIQUE (caller, idempotency_key) before scaling out.
_idempotency = {}


async def handle_batch(batch: BatchRequest, scheduler, caller, idempotency_key):
    """Process a batch rescheduling request. Replays of the same key return
    the stored response instead of rescheduling again."""
    store_key = (caller, idempotency_key)
    fingerprint = batch.fingerprint()
    if store_key in _idempotency:
        stored_fingerprint, response = _idempotency[store_key]
        if stored_fingerprint != fingerprint:
            raise HTTPException(422, "Idempotency-Key reused with a different payload")
        if response is _IN_FLIGHT:
            raise HTTPException(409, "A request with this Idempotency-Key is in progress")
        return response

    _idempotency[store_key] = (fingerprint, _IN_FLIGHT)
    try:
        response = await _run_batch(batch, scheduler, caller, idempotency_key)
    except BaseException:
        del _idempotency[store_key]
        raise
    _idempotency[store_key] = (fingerprint, response)
    return response


async def _run_batch(batch, scheduler, caller, idempotency_key):
    batch_id = str(uuid.uuid4())
    started = time.monotonic()
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async def run_one(job):
        async with semaphore:
            return await asyncio.to_thread(
                _process_job, job, scheduler, caller, idempotency_key
            )

    results = await asyncio.gather(*(run_one(job) for job in batch.jobs))
    log.info(
        "reschedule batch finished",
        extra={
            "batch_id": batch_id,
            "jobs": len(results),
            "not_rescheduled": sum(r["status"] != "rescheduled" for r in results),
            "duration_ms": round((time.monotonic() - started) * 1000),
        },
    )
    return {"batch_id": batch_id, "results": results}


def _process_job(job: Job, scheduler, caller, idempotency_key):
    """Never raises: one bad job must not abort the rest of the batch."""
    if not can_reschedule(caller, job.job_id):
        return _result(job, "forbidden", "caller may not reschedule this job")

    result = _reschedule(job, scheduler, idempotency_key)
    try:
        audit.write_reschedule_event(job.job_id, result["status"], result["error"])
    except Exception:
        log.exception("audit write failed for job %s", job.job_id)
    return result


def _reschedule(job, scheduler, idempotency_key):
    try:
        response = scheduler.reschedule(job_id=job.job_id, requested_at=job.requested_at)
    except grpc.RpcError as e:
        return _result(job, "failed", f"scheduler error: {e.code().name}")
    except Exception:
        log.exception("unexpected error rescheduling job %s", job.job_id)
        return _result(job, "failed", "internal error")

    if not response.success:
        return _result(job, "failed", "scheduler rejected the reschedule")

    try:
        publish_event("job.rescheduled", {
            # Stable per (request, job) so consumers can dedupe (AURORA-1247).
            "event_id": f"{idempotency_key}:{job.job_id}",
            "job_id": job.job_id,
            "new_schedule": response.new_schedule_id,
        })
    except Exception:
        # ponytail: no outbox yet, the job moved but nobody was notified.
        # Surface it to the caller; transactional outbox is the real fix.
        log.exception("job.rescheduled publish failed for job %s", job.job_id)
        return _result(job, "rescheduled_event_pending", "job.rescheduled not published")
    return _result(job, "rescheduled")


def _result(job, status, error=None):
    return {"job_id": job.job_id, "status": status, "error": error}
