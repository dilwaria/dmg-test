# pylint: disable=unused-argument,protected-access  # fakes mirror real signatures; fixture resets handler state
from types import SimpleNamespace

import grpc
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.rescheduling import api, handler, scheduler_client
from services.rescheduling.auth import get_caller
from services.rescheduling.models import MAX_BATCH_SIZE
from services.rescheduling.scheduler_client import NewSchedulerV2Client, get_scheduler


class FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeScheduler:
    """Succeeds unless job_id is in `outcomes`: False means rejected, an
    exception instance means raised."""

    def __init__(self, outcomes=None):
        self.outcomes = outcomes or {}
        self.calls = []

    def reschedule(self, job_id, requested_at):
        self.calls.append(job_id)
        outcome = self.outcomes.get(job_id, True)
        if isinstance(outcome, Exception):
            raise outcome
        return SimpleNamespace(success=outcome, new_schedule_id=f"s-{job_id}")


@pytest.fixture
def env(monkeypatch):
    handler._idempotency.clear()
    state = SimpleNamespace(
        scheduler=FakeScheduler(), published=[], audited=[], allowed=True, flag=True
    )
    monkeypatch.setattr(handler, "can_reschedule", lambda caller, job_id: state.allowed)
    monkeypatch.setattr(handler, "publish_event", lambda topic, payload: state.published.append(payload))
    monkeypatch.setattr(handler.audit, "write_reschedule_event", lambda *args: state.audited.append(args))
    monkeypatch.setattr(api, "feature_flag", lambda name, ctx=None: state.flag)

    app = FastAPI()
    app.include_router(api.router)
    app.dependency_overrides[get_caller] = lambda: "customer-1"
    app.dependency_overrides[get_scheduler] = lambda: state.scheduler
    state.client = TestClient(app)
    return state


def post(env, *job_ids, key="key-1"):
    jobs = [{"job_id": j, "requested_at": "2026-05-22"} for j in job_ids]
    headers = {"Idempotency-Key": key} if key else {}
    return env.client.post("/reschedule/batch", json={"jobs": jobs}, headers=headers)


def statuses(response):
    return {r["job_id"]: r["status"] for r in response.json()["results"]}


def test_happy_path_reschedules_publishes_and_audits(env):
    response = post(env, "1", "2")
    assert response.status_code == 200
    assert statuses(response) == {"1": "rescheduled", "2": "rescheduled"}
    assert sorted(p["job_id"] for p in env.published) == ["1", "2"]
    assert {p["event_id"] for p in env.published} == {"key-1:1", "key-1:2"}
    assert len(env.audited) == 2


def test_one_failure_does_not_abort_batch(env):
    env.scheduler.outcomes = {
        "2": False,
        "3": FakeRpcError(grpc.StatusCode.INTERNAL),
        "4": RuntimeError("boom"),
    }
    response = post(env, "1", "2", "3", "4", "5")
    assert response.status_code == 200
    assert statuses(response) == {
        "1": "rescheduled", "2": "failed", "3": "failed", "4": "failed", "5": "rescheduled",
    }
    errors = {r["job_id"]: r["error"] for r in response.json()["results"]}
    assert errors["3"] == "scheduler error: INTERNAL"
    assert sorted(p["job_id"] for p in env.published) == ["1", "5"]
    assert len(env.audited) == 5


def test_publish_failure_is_reported_not_hidden(env, monkeypatch):
    def fail(topic, payload):
        raise RuntimeError("kafka down")

    monkeypatch.setattr(handler, "publish_event", fail)
    assert statuses(post(env, "1")) == {"1": "rescheduled_event_pending"}


def test_audit_failure_does_not_change_result(env, monkeypatch):
    def fail(*args):
        raise RuntimeError("db down")

    monkeypatch.setattr(handler.audit, "write_reschedule_event", fail)
    assert statuses(post(env, "1")) == {"1": "rescheduled"}


def test_replay_with_same_key_does_not_reschedule_again(env):
    first = post(env, "1", "2")
    second = post(env, "1", "2")
    assert second.status_code == 200
    assert second.json() == first.json()
    assert env.scheduler.calls.count("1") == 1
    assert len(env.published) == 2


def test_same_key_different_payload_is_rejected(env):
    post(env, "1")
    assert post(env, "2").status_code == 422


def test_forbidden_jobs_never_reach_scheduler(env):
    env.allowed = False
    assert statuses(post(env, "1")) == {"1": "forbidden"}
    assert env.scheduler.calls == []
    assert env.published == []


@pytest.mark.parametrize("job_ids", [(), ("1", "1"), tuple(str(i) for i in range(MAX_BATCH_SIZE + 1))])
def test_invalid_batches_are_rejected(env, job_ids):
    assert post(env, *job_ids).status_code == 422
    assert env.scheduler.calls == []


def test_missing_idempotency_key_is_rejected(env):
    assert post(env, "1", key=None).status_code == 422


def test_flag_off_returns_404(env):
    env.flag = False
    assert post(env, "1").status_code == 404
    assert env.scheduler.calls == []


class FakeStub:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def Reschedule(self, request, timeout):
        assert timeout == scheduler_client.RPC_TIMEOUT_S
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr(scheduler_client, "RETRY_BACKOFF_S", 0)


def test_client_retries_unavailable():
    ok = SimpleNamespace(success=True)
    stub = FakeStub(FakeRpcError(grpc.StatusCode.UNAVAILABLE), ok)
    assert NewSchedulerV2Client(stub).reschedule("1", "2026-05-22") is ok
    assert stub.calls == 2


def test_client_gives_up_after_max_retries():
    stub = FakeStub(*[FakeRpcError(grpc.StatusCode.UNAVAILABLE)] * (scheduler_client.MAX_RETRIES + 1))
    with pytest.raises(grpc.RpcError):
        NewSchedulerV2Client(stub).reschedule("1", "2026-05-22")
    assert stub.calls == scheduler_client.MAX_RETRIES + 1


def test_client_does_not_retry_deadline_exceeded():
    stub = FakeStub(FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED))
    with pytest.raises(grpc.RpcError):
        NewSchedulerV2Client(stub).reschedule("1", "2026-05-22")
    assert stub.calls == 1
