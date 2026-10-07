# Stand-in for the generated protobuf module.
from dataclasses import dataclass


@dataclass
class RescheduleRequest:
    job_id: str
    requested_at: str
