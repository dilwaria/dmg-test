import hashlib

from pydantic import BaseModel, Field, field_validator

# Sync endpoint cap. Raise toward 1000 only once the async batch path exists.
MAX_BATCH_SIZE = 100


class Job(BaseModel):
    job_id: str = Field(min_length=1)
    requested_at: str = Field(min_length=1)


class BatchRequest(BaseModel):
    jobs: list[Job] = Field(min_length=1, max_length=MAX_BATCH_SIZE)

    @field_validator("jobs")
    @classmethod
    def unique_job_ids(cls, jobs):
        ids = [job.job_id for job in jobs]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate job_id in batch")
        return jobs

    def fingerprint(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()
