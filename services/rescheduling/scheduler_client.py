import time
from functools import lru_cache

import grpc

from .scheduler_pb2 import RescheduleRequest

RPC_TIMEOUT_S = 2.0
MAX_RETRIES = 2
RETRY_BACKOFF_S = 0.1
# Only retry when the call never reached the scheduler. DEADLINE_EXCEEDED may
# have applied the reschedule, so retrying it could double-reschedule.
RETRYABLE_CODES = {grpc.StatusCode.UNAVAILABLE}


class NewSchedulerV2Client:
    def __init__(self, grpc_stub):
        self.grpc_stub = grpc_stub

    def reschedule(self, job_id, requested_at):
        """Raises grpc.RpcError once retries are exhausted or the code is fatal."""
        request = RescheduleRequest(job_id=job_id, requested_at=requested_at)
        for attempt in range(MAX_RETRIES + 1):
            try:
                return self.grpc_stub.Reschedule(request, timeout=RPC_TIMEOUT_S)
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE_CODES or attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_BACKOFF_S * 2**attempt)


@lru_cache
def get_scheduler():
    # Must return the same scheduler router the single-job /reschedule path
    # uses (legacy adapter), so jobs still on OldSchedulerV1 are not sent to V2.
    raise NotImplementedError("Wire the scheduler router used by /reschedule here")
