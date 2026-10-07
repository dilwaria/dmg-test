from fastapi import Header, HTTPException


# Stand-in: replace with the real auth dependency.
def get_caller(x_caller_id: str | None = Header(default=None)) -> str:
    if not x_caller_id:
        raise HTTPException(401, "Unauthenticated")
    return x_caller_id


def can_reschedule(caller: str, job_id: str) -> bool:
    # Stand-in: check the caller owns the job. Fails closed until wired.
    return False
