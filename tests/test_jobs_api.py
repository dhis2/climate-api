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
from climate_api.processing import services as processing_services
from tests.helpers import dataset_record


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
        response = client.get(f"/internal/jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"successful", "failed", "cancelled"}:
            return payload
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for job {job_id} to complete")


def test_async_process_execution_creates_job_and_exposes_status(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    response = client.post(
        "/processes/resample/execution",
        headers={"Prefer": "respond-async"},
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "publish": True,
        },
    )

    assert response.status_code == 202
    payload = cast(dict[str, Any], response.json())
    job_id = payload["jobID"]
    assert response.headers["Location"] == f"/internal/jobs/{job_id}"
    assert payload["processID"] == "resample"

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.SUCCESSFUL
    result = cast(dict[str, Any], finished["result"])
    assert result["artifact_id"] == "artifact-123"

    jobs_response = client.get("/internal/jobs")
    assert jobs_response.status_code == 200
    jobs_payload = cast(dict[str, Any], jobs_response.json())
    assert any(item["jobID"] == job_id for item in jobs_payload["jobs"])


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

    cancel_response = client.delete(f"/internal/jobs/{job_id}")
    assert cancel_response.status_code == 200
    assert cancel_response.json()["cancelRequested"] is True

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.CANCELLED


def test_delete_unknown_internal_job_returns_404(client: TestClient) -> None:
    response = client.delete("/internal/jobs/missing-job")

    assert response.status_code == 404


def test_delete_terminal_job_leaves_record_unchanged(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    response = client.post(
        "/processes/resample/execution",
        headers={"Prefer": "respond-async"},
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
        },
    )
    assert response.status_code == 202
    job_id = cast(dict[str, Any], response.json())["jobID"]

    finished = _wait_for_terminal_job(client, job_id)
    assert finished["status"] == JobStatus.SUCCESSFUL

    cancel_response = client.delete(f"/internal/jobs/{job_id}")
    assert cancel_response.status_code == 200
    payload = cast(dict[str, Any], cancel_response.json())
    assert payload["status"] == JobStatus.SUCCESSFUL
    assert payload["cancelRequested"] is False


def test_async_submission_rejects_invalid_resample_input(client: TestClient) -> None:
    response = client.post(
        "/processes/resample/execution",
        headers={"Prefer": "respond-async"},
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "invalid",
            "method": "sum",
            "start": "2026-01-05",
        },
    )

    assert response.status_code == 400
    jobs_response = client.get("/jobs")
    assert jobs_response.status_code == 200
    jobs_payload = cast(dict[str, Any], jobs_response.json())
    assert jobs_payload["jobs"] == []
