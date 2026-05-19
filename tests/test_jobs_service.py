from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

import climate_api.data_registry.services.processes as registry_processes
import climate_api.jobs.service as jobs_service
from climate_api.jobs import store as job_store
from climate_api.jobs.models import JobRecord, JobStatus
from climate_api.jobs.service import JobService, ProcessExecutor, ThreadProcessExecutor, _retry_delay_seconds


@pytest.fixture(autouse=True)
def _temp_jobs_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(job_store, "JOBS_DIR", jobs_dir)
    monkeypatch.setattr(job_store, "JOBS_INDEX_PATH", jobs_dir / "jobs.json")


def _job_record(job_id: str = "job-1", *, status: JobStatus = JobStatus.ACCEPTED) -> JobRecord:
    return JobRecord(
        job_id=job_id,
        process_id="recoverable-process",
        status=status,
        created_at=datetime(2026, 5, 18, tzinfo=UTC),
        request={"value": 2},
        links=[],
    )


def test_store_create_get_and_list_round_trip() -> None:
    created = job_store.create_job_record(_job_record())

    fetched = job_store.get_job_record(created.job_id)
    listed = job_store.list_job_records()

    assert fetched is not None
    assert fetched.job_id == created.job_id
    assert [record.job_id for record in listed] == [created.job_id]


def test_store_upsert_replaces_existing_record() -> None:
    job_store.create_job_record(_job_record())

    updated = _job_record(status=JobStatus.RUNNING).model_copy(update={"attempt": 1})
    job_store.upsert_job_record(updated)

    fetched = job_store.get_job_record(updated.job_id)
    assert fetched is not None
    assert fetched.status == JobStatus.RUNNING
    assert fetched.attempt == 1


def test_store_mutate_missing_job_raises_key_error() -> None:
    with pytest.raises(KeyError):
        job_store.mutate_job_record("missing", lambda current: current)


def test_retry_delay_seconds_matches_design_schedule() -> None:
    assert _retry_delay_seconds(1) == 60
    assert _retry_delay_seconds(2) == 120
    assert _retry_delay_seconds(3) == 240
    assert _retry_delay_seconds(4) == 240


def test_service_save_cursor_persists_checkpoint() -> None:
    service = JobService()
    try:
        job_store.create_job_record(_job_record())

        service.save_cursor("job-1", {"last_period": "2024-03"})

        fetched = job_store.get_job_record("job-1")
        assert fetched is not None
        assert fetched.cursor == {"last_period": "2024-03"}
    finally:
        service.shutdown()


def test_service_update_progress_persists_percent() -> None:
    service = JobService()
    try:
        job_store.create_job_record(_job_record())

        service.update_progress("job-1", done=1, total=4, message="Working")

        fetched = job_store.get_job_record("job-1")
        assert fetched is not None
        assert fetched.progress.done == 1
        assert fetched.progress.total == 4
        assert fetched.progress.percent == 25.0
        assert fetched.progress.message == "Working"
    finally:
        service.shutdown()


def test_service_uses_executor_kind_on_created_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeExecutor:
        kind = "fake-thread"

        def __init__(self) -> None:
            self._delegate = ThreadProcessExecutor(max_workers=1)

        def submit(self, fn: Any, /, *args: Any, **kwargs: Any):
            return self._delegate.submit(fn, *args, **kwargs)

        def shutdown(self) -> None:
            self._delegate.shutdown()

    service = JobService(executor=cast(ProcessExecutor, FakeExecutor()))
    try:

        def fake_get_process(process_id: str) -> dict[str, object] | None:
            if process_id != "recoverable-process":
                return None
            return {
                "id": process_id,
                "title": "Recoverable process",
                "execution": {"function": "tests.fake.recoverable"},
                "expose": True,
                "jobControlOptions": ["sync-execute", "async-execute"],
            }

        monkeypatch.setattr(jobs_service.process_registry, "get_process", fake_get_process)
        monkeypatch.setattr(
            registry_processes,
            "_get_dynamic_function",
            lambda path: lambda *, value: {"value": value},
        )

        created = service.submit_process_job(process_id="recoverable-process", request={"value": 2})
        assert created.executor_kind == "fake-thread"
    finally:
        service.shutdown()


def test_recover_pending_jobs_requeues_accepted_process(monkeypatch: pytest.MonkeyPatch) -> None:
    service = JobService(max_workers=1)
    try:
        job_store.create_job_record(_job_record())

        def fake_get_process(process_id: str) -> dict[str, object] | None:
            if process_id != "recoverable-process":
                return None
            return {
                "id": process_id,
                "title": "Recoverable process",
                "execution": {"function": "tests.fake.recoverable"},
                "expose": True,
                "jobControlOptions": ["sync-execute", "async-execute"],
            }

        monkeypatch.setattr("climate_api.jobs.service.process_registry.get_process", fake_get_process)
        monkeypatch.setattr(
            "climate_api.data_registry.services.processes._get_dynamic_function",
            lambda path: lambda *, value: {"value": value * 2},
        )

        service.recover_pending_jobs()

        for _ in range(100):
            record = job_store.get_job_record("job-1")
            if record is not None and record.status == JobStatus.SUCCESSFUL:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("Recovered job did not complete")

        record = job_store.get_job_record("job-1")
        assert record is not None
        assert record.status == JobStatus.SUCCESSFUL
        assert record.result == {"value": 4}
    finally:
        service.shutdown()


def test_recover_pending_jobs_requeues_retrying_process(monkeypatch: pytest.MonkeyPatch) -> None:
    service = JobService(max_workers=1)
    try:
        job_store.create_job_record(_job_record(status=JobStatus.RETRYING))

        def fake_get_process(process_id: str) -> dict[str, object] | None:
            if process_id != "recoverable-process":
                return None
            return {
                "id": process_id,
                "title": "Recoverable process",
                "execution": {"function": "tests.fake.recoverable"},
                "expose": True,
                "jobControlOptions": ["sync-execute", "async-execute"],
            }

        monkeypatch.setattr("climate_api.jobs.service.process_registry.get_process", fake_get_process)
        monkeypatch.setattr(
            "climate_api.data_registry.services.processes._get_dynamic_function",
            lambda path: lambda *, value: {"value": value * 3},
        )

        service.recover_pending_jobs()

        for _ in range(100):
            record = job_store.get_job_record("job-1")
            if record is not None and record.status == JobStatus.SUCCESSFUL:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("Recovered retrying job did not complete")

        record = job_store.get_job_record("job-1")
        assert record is not None
        assert record.status == JobStatus.SUCCESSFUL
        assert record.result == {"value": 6}
    finally:
        service.shutdown()


def test_retrying_job_exhausts_attempts_and_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    service = JobService(max_workers=1)
    try:
        monkeypatch.setattr("climate_api.jobs.service.time.sleep", lambda _seconds: None)

        def fake_get_process(process_id: str) -> dict[str, object] | None:
            if process_id != "recoverable-process":
                return None
            return {
                "id": process_id,
                "title": "Recoverable process",
                "execution": {"function": "tests.fake.failing"},
                "expose": True,
                "jobControlOptions": ["sync-execute", "async-execute"],
            }

        monkeypatch.setattr("climate_api.jobs.service.process_registry.get_process", fake_get_process)
        monkeypatch.setattr(
            "climate_api.data_registry.services.processes._get_dynamic_function",
            lambda path: lambda *, value: (_ for _ in ()).throw(RuntimeError(f"boom:{value}")),
        )

        created = service.submit_process_job(process_id="recoverable-process", request={"value": 2}, max_attempts=2)

        for _ in range(100):
            record = job_store.get_job_record(created.job_id)
            if record is not None and record.status == JobStatus.FAILED:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("Retrying job did not fail in time")

        record = job_store.get_job_record(created.job_id)
        assert record is not None
        assert record.status == JobStatus.FAILED
        assert record.attempt == 2
        assert record.error is not None
        assert record.error.type == "RuntimeError"
    finally:
        service.shutdown()


def test_retry_wait_honors_cancellation_request(monkeypatch: pytest.MonkeyPatch) -> None:
    service = JobService(max_workers=1)
    try:

        def fake_get_process(process_id: str) -> dict[str, object] | None:
            if process_id != "recoverable-process":
                return None
            return {
                "id": process_id,
                "title": "Recoverable process",
                "execution": {"function": "tests.fake.failing"},
                "expose": True,
                "jobControlOptions": ["sync-execute", "async-execute"],
            }

        monkeypatch.setattr("climate_api.jobs.service.process_registry.get_process", fake_get_process)
        monkeypatch.setattr(
            "climate_api.data_registry.services.processes._get_dynamic_function",
            lambda path: lambda *, value: (_ for _ in ()).throw(RuntimeError(f"boom:{value}")),
        )

        sleep_calls = {"count": 0}

        def fake_sleep(_seconds: float) -> None:
            sleep_calls["count"] += 1
            if sleep_calls["count"] == 1:
                service.request_cancellation(created.job_id)

        monkeypatch.setattr("climate_api.jobs.service.time.sleep", fake_sleep)

        created = service.submit_process_job(process_id="recoverable-process", request={"value": 2}, max_attempts=2)

        for _ in range(100):
            record = job_store.get_job_record(created.job_id)
            if record is not None and record.status == JobStatus.CANCELLED:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("Retrying job did not cancel in time")

        record = job_store.get_job_record(created.job_id)
        assert record is not None
        assert record.status == JobStatus.CANCELLED
        assert record.progress.message == "Cancelled before retry execution resumed"
    finally:
        service.shutdown()
