from eo_api.ogc_async import enrich_async_job_payload


def test_enrich_async_job_payload_with_relative_location() -> None:
    payload = {"id": "abc123", "type": "process", "status": "accepted"}
    enriched = enrich_async_job_payload(
        payload,
        location="/ogcapi/jobs/abc123",
        base_url="http://127.0.0.1:8000/",
    )
    assert enriched["jobID"] == "abc123"
    assert enriched["jobUrl"] == "http://127.0.0.1:8000/ogcapi/jobs/abc123"
    assert enriched["resultsUrl"] == "http://127.0.0.1:8000/ogcapi/jobs/abc123/results"


def test_enrich_async_job_payload_with_absolute_location() -> None:
    payload = {"id": "xyz999", "type": "process", "status": "accepted"}
    enriched = enrich_async_job_payload(
        payload,
        location="http://localhost:8000/ogcapi/jobs/xyz999",
        base_url="http://127.0.0.1:8000/",
    )
    assert enriched["jobID"] == "xyz999"
    assert enriched["jobUrl"] == "http://localhost:8000/ogcapi/jobs/xyz999"
    assert enriched["resultsUrl"] == "http://localhost:8000/ogcapi/jobs/xyz999/results"
