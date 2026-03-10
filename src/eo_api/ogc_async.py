"""Helpers for OGC async execution response enrichment."""

from __future__ import annotations

from urllib.parse import urljoin


def enrich_async_job_payload(payload: dict, *, location: str | None, base_url: str) -> dict:
    """Add convenient absolute job links to OGC async accepted payloads."""
    job_id = str(payload.get("id", "")).strip()
    if not job_id:
        return payload

    if location:
        if location.startswith("http://") or location.startswith("https://"):
            job_url = location
        else:
            job_url = urljoin(base_url, location.lstrip("/"))
    else:
        job_url = urljoin(base_url, f"ogcapi/jobs/{job_id}")

    enriched = dict(payload)
    enriched.setdefault("jobID", job_id)
    enriched.setdefault("jobUrl", job_url)
    enriched.setdefault("resultsUrl", f"{job_url}/results")
    return enriched
