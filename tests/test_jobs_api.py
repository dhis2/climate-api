from __future__ import annotations

import time
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from climate_api.jobs import store as job_store
from climate_api.jobs.models import JobCancelledError, JobStatus
from climate_api.jobs.service import reset_job_service


@pytest.fixture(autouse=True)
def _temp_jobs_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(job_store, "JOBS_DIR", jobs_dir)
    monkeypatch.setattr(job_store, "JOBS_INDEX_PATH", jobs_dir / "jobs.json")
    reset_job_service()
    yield
    reset_job_service()


def _wait_for_terminal_job(client: TestClient, job_id: str, *, timeout: float = 3.0) -> dict[str, object]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"successful", "failed", "cancelled"}:
            return payload
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for job {job_id} to complete")


def _fake_process(process_id: str = "quick-process") -> dict[str, object]:
    return {
        "id": process_id,
        "title": "Quick process",
        "description": "Fast fake process for job lifecycle tests",
        "execution": {"function": "tests.fake.quick_process"},
        "expose": True,
        "jobControlOptions": ["sync-execute", "async-execute"],
    }


def test_async_process_execution_creates_job_and_exposes_status(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda pid: _fake_process(pid) if pid == "quick-process" else None,
    )
    monkeypatch.setattr(
        "climate_api.jobs.service.process_registry.get_process",
        lambda pid: _fake_process(pid) if pid == "quick-process" else None,
    )
    monkeypatch.setattr(
        "climate_api.data_registry.services.processes._get_dynamic_function",
        lambda path: lambda **kw: {"status": "completed", "result": "ok"},
    )

    response = client.post(
        "/processes/quick-process/execution",
        headers={"Prefer": "respond-async"},
        json={"param": "value"},
    )

    assert response.status_code == 202
    payload = cast(dict[str, Any], response.json())
    job_id = payload["jobID"]
    assert response.headers["Location"] == f"/internal/jobs/{job_id}"
    assert payload["processID"] == "quick-process"

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.SUCCESSFUL
    assert cast(dict[str, Any], finished["result"])["status"] == "completed"

    jobs_response = client.get("/jobs")
    assert jobs_response.status_code == 200
    assert any(item["jobID"] == job_id for item in cast(dict[str, Any], jobs_response.json())["jobs"])


def test_delete_job_requests_cooperative_cancellation(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get_process(process_id: str) -> dict[str, object] | None:
        if process_id != "slow-process":
            return None
        return {
            "id": process_id,
            "title": "Slow process",
            "description": "Test cancellation",
            "execution": {"function": "tests.fake.slow_process"},
            "expose": True,
            "jobControlOptions": ["sync-execute", "async-execute"],
        }

    def slow_process(
        *,
        on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
        is_cancel_requested: Callable[[], bool] | None = None,
        **_kwargs: object,
    ) -> dict[str, object]:
        for step in range(20):
            if is_cancel_requested is not None and is_cancel_requested():
                raise JobCancelledError()
            if on_progress is not None:
                on_progress(step, 20, "Working")
            time.sleep(0.01)
        return {"ok": True}

    monkeypatch.setattr("climate_api.processing.routes.process_registry.get_process", fake_get_process)
    monkeypatch.setattr("climate_api.jobs.service.process_registry.get_process", fake_get_process)
    monkeypatch.setattr(
        "climate_api.data_registry.services.processes._get_dynamic_function",
        lambda path: slow_process,
    )

    response = client.post("/processes/slow-process/execution", headers={"Prefer": "respond-async"}, json={})
    assert response.status_code == 202
    job_id = response.json()["jobID"]

    cancel_response = client.delete(f"/jobs/{job_id}")
    assert cancel_response.status_code == 200
    assert cancel_response.json()["cancelRequested"] is True

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.CANCELLED


def test_delete_unknown_job_returns_404(client: TestClient) -> None:
    response = client.delete("/jobs/missing-job")

    assert response.status_code == 404


def test_delete_terminal_job_leaves_record_unchanged(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "climate_api.processing.routes.process_registry.get_process",
        lambda pid: _fake_process(pid) if pid == "quick-process" else None,
    )
    monkeypatch.setattr(
        "climate_api.jobs.service.process_registry.get_process",
        lambda pid: _fake_process(pid) if pid == "quick-process" else None,
    )
    monkeypatch.setattr(
        "climate_api.data_registry.services.processes._get_dynamic_function",
        lambda path: lambda **kw: {"status": "completed"},
    )

    response = client.post(
        "/processes/quick-process/execution",
        headers={"Prefer": "respond-async"},
        json={},
    )
    assert response.status_code == 202
    job_id = cast(dict[str, Any], response.json())["jobID"]

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.SUCCESSFUL

    cancel_response = client.delete(f"/jobs/{job_id}")
    assert cancel_response.status_code == 200
    payload = cast(dict[str, Any], cancel_response.json())
    assert payload["status"] == JobStatus.SUCCESSFUL
    assert payload["cancelRequested"] is False
