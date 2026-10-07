# CLAUDE.md — ReschedulingEngine (Team Aurora)

## Stack
- Python 3.13, FastAPI, pydantic v2, grpcio, pytest. Deps in requirements.txt, venv at .venv/.
- Code: services/rescheduling/. Tests: tests/.
- Storage: PostgreSQL `reschedule_events` (audit log). Events: Kafka `job.rescheduled`.
- Downstream: SchedulerV2 over gRPC. ~15% of traffic still on OldSchedulerV1 via legacy adapter (AURORA-1102).

## Commands
- Test: `.venv/bin/python -m pytest -q tests`. Must pass before any commit.

## Done means
- New or changed logic has a test that fails without the change.
- Every outbound gRPC call has a timeout. Retry only on UNAVAILABLE.
- Endpoints that change state require an Idempotency-Key.
- New endpoints sit behind a feature flag that defaults to off.

## Never
- Run deploy.sh or touch prod config, prod data or secrets.
- Put credentials in code, config or this file.
- Change storage engines or add dependencies without approval in the ticket.

## Ask first
- Pushing to remote, schema migrations, deleting files, changing public API contracts.

## Do without asking
- Read code, run tests, edit files within the ticket's scope, open draft PRs.

## Disagreement
- If my approach has a correctness, security or scale risk, say so once with the reason. Then follow my decision.

## Known traps
- Idempotency is NOT handled by the scheduler (old design doc is wrong).
- An async route must not make blocking DB or gRPC calls.

Owner: Aurora EM. Review every sprint retro.
