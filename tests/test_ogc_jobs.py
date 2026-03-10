from eo_api.ogc_jobs import apply_jobs_process_filter


def test_apply_jobs_process_filter_keeps_matching_jobs() -> None:
    jobs = [
        {"identifier": "1", "process_id": "generic-dhis2-workflow"},
        {"identifier": "2", "process_id": "chirps3-download"},
        {"identifier": "3", "process_id": "generic-dhis2-workflow"},
    ]
    filtered = apply_jobs_process_filter(jobs, process_id="generic-dhis2-workflow")
    assert [item["identifier"] for item in filtered] == ["1", "3"]
