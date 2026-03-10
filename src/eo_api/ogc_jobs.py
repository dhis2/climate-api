"""Helpers for OGC jobs filtering."""

from __future__ import annotations

from typing import Any


def _normalize_process_id(process_id: str | None) -> str | None:
    normalized = (process_id or "").strip()
    return normalized or None


def _matches_process_id(job: dict[str, Any], process_id: str) -> bool:
    for key in ("process_id", "processID", "process"):
        value = job.get(key)
        if isinstance(value, str) and value == process_id:
            return True
    return False


def apply_jobs_process_filter(jobs: list[dict[str, Any]], process_id: str | None) -> list[dict[str, Any]]:
    """Filter jobs list by process identifier."""
    normalized = _normalize_process_id(process_id)
    if normalized is None:
        return jobs
    return [job for job in jobs if _matches_process_id(job, normalized)]


def install_processes_jobs_filter_patch() -> None:
    """Patch pygeoapi jobs endpoint to support process-scoped filtering before pagination."""
    from pygeoapi.api import processes as processes_api

    if getattr(processes_api, "_eo_api_jobs_filter_patch_installed", False):
        return

    original_get_jobs = processes_api.get_jobs

    def _patched_get_jobs(api, request, job_id=None):  # type: ignore[no-untyped-def]
        process_id = request.params.get("process_id") or request.params.get("processID")
        normalized_process_id = _normalize_process_id(process_id)

        if job_id is not None or normalized_process_id is None:
            return original_get_jobs(api, request, job_id)

        original_manager_get_jobs = api.manager.get_jobs

        def _filtered_manager_get_jobs(status=None, limit=None, offset=None):  # type: ignore[no-untyped-def]
            result = original_manager_get_jobs(status=status, limit=None, offset=0)
            jobs = result.get("jobs", [])
            if not isinstance(jobs, list):
                jobs = []
            filtered = apply_jobs_process_filter(jobs, normalized_process_id)
            number_matched = len(filtered)

            if offset:
                filtered = filtered[offset:]
            if limit:
                filtered = filtered[:limit]

            return {
                "jobs": filtered,
                "numberMatched": number_matched,
            }

        api.manager.get_jobs = _filtered_manager_get_jobs
        try:
            return original_get_jobs(api, request, job_id)
        finally:
            api.manager.get_jobs = original_manager_get_jobs

    processes_api.get_jobs = _patched_get_jobs
    setattr(processes_api, "_eo_api_jobs_filter_patch_installed", True)
