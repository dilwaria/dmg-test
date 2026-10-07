from fastapi import APIRouter, Depends, Header, HTTPException

from .auth import get_caller
from .feature_flags import feature_flag
from .handler import handle_batch
from .models import BatchRequest
from .scheduler_client import get_scheduler

router = APIRouter()


@router.post("/reschedule/batch")
async def reschedule_batch(
    req: BatchRequest,
    idempotency_key: str = Header(min_length=1, max_length=255),
    caller: str = Depends(get_caller),
    scheduler=Depends(get_scheduler),
):
    if not feature_flag("enable_v2_reschedule", ctx={"caller": caller}):
        raise HTTPException(404)
    return await handle_batch(req, scheduler, caller, idempotency_key)
