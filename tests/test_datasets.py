from datetime import UTC, datetime
from pathlib import Path

import pytest

from climate_api.ingestions import services
from climate_api.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    DatasetDetailRecord,
    DatasetPublication,
    PublicationStatus,
)
from climate_api.publications.services import managed_dataset_id_for


@pytest.fixture(autouse=True)
def materialized_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(services, "_artifact_storage_exists", lambda _: True)


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        asset_paths=["/tmp/chirps3_precipitation_daily.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end=end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime.fromisoformat(created_at),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def _dataset_detail(dataset_id: str) -> DatasetDetailRecord:
    return DatasetDetailRecord(
        dataset_id=dataset_id,
        source_dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        short_name="CHIRPS3 precip",
        variable="precip",
        period_type="daily",
        units="mm",
        resolution="5 km x 5 km",
        source="CHIRPS v3",
        source_url="https://example.com/chirps",
        extent=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-11"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        last_updated=datetime(2026, 1, 11, tzinfo=UTC),
        links=[],
        publication=DatasetPublication(
            status=PublicationStatus.PUBLISHED,
            published_at=datetime(2026, 1, 11, tzinfo=UTC),
        ),
        versions=[],
    )


def test_list_datasets_groups_artifacts_by_managed_dataset_id(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _artifact(artifact_id="a1", created_at="2026-01-10T00:00:00+00:00", end="2026-01-10"),
        _artifact(artifact_id="a2", created_at="2026-01-11T00:00:00+00:00", end="2026-01-11"),
    ]
    monkeypatch.setattr(services, "_load_records", lambda: records)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "short_name": "CHIRPS3 precip",
            "period_type": "daily",
            "units": "mm",
            "resolution": "5 km x 5 km",
            "source": "CHIRPS v3",
            "source_url": "https://example.com/chirps",
        },
    )

    result = services.list_datasets()

    assert len(result.items) == 1
    dataset = result.items[0]
    assert dataset.dataset_id == "chirps3_precipitation_daily"
    assert dataset.source_dataset_id == "chirps3_precipitation_daily"
    assert dataset.period_type == "daily"
    assert dataset.units == "mm"
    assert dataset.extent.temporal.end == "2026-01-11"
    assert dataset.publication.status == PublicationStatus.PUBLISHED
    assert any(link.href == f"/zarr/{dataset.dataset_id}" for link in dataset.links)
    assert any(link.href == f"/stac/collections/{dataset.dataset_id}" for link in dataset.links)


def test_dataset_links_include_stac_for_published_zarr() -> None:
    links = services._dataset_links("chirps3_precipitation_daily", _artifact(artifact_id="a1"))

    assert any(link.rel == "stac" and link.href == "/stac/collections/chirps3_precipitation_daily" for link in links)


def test_dataset_links_include_stac_for_published_icechunk() -> None:
    artifact = _artifact(artifact_id="a1")
    artifact = artifact.model_copy(update={"format": ArtifactFormat.ICECHUNK})

    links = services._dataset_links("chirps3_precipitation_daily", artifact)

    assert any(link.rel == "stac" and link.href == "/stac/collections/chirps3_precipitation_daily" for link in links)


def test_dataset_links_omit_stac_for_unpublished() -> None:
    unpublished = _artifact(artifact_id="a1")
    unpublished.publication.status = PublicationStatus.UNPUBLISHED

    unpublished_links = services._dataset_links("chirps3_precipitation_daily", unpublished)

    assert all(link.rel != "stac" for link in unpublished_links)


def test_list_ingestions_returns_most_recent_first(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _artifact(artifact_id="a1", created_at="2026-01-10T00:00:00+00:00", end="2026-01-10"),
        _artifact(artifact_id="a2", created_at="2026-01-11T00:00:00+00:00", end="2026-01-11"),
    ]
    monkeypatch.setattr(services, "_load_records", lambda: records)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "short_name": "CHIRPS3 precip",
            "period_type": "daily",
            "units": "mm",
            "resolution": "5 km x 5 km",
            "source": "CHIRPS v3",
            "source_url": "https://example.com/chirps",
        },
    )

    result = services.list_ingestions()

    assert result.kind == "IngestionList"
    assert [item.ingestion_id for item in result.items] == ["a2", "a1"]
    assert result.items[0].dataset.dataset_id == "chirps3_precipitation_daily"


def test_managed_dataset_id_is_derived_from_dataset_id(monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = _artifact(artifact_id="a1")

    assert managed_dataset_id_for(artifact) == "chirps3_precipitation_daily"


def test_mutate_records_persists_mutation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    monkeypatch.setattr(services, "ARTIFACTS_DIR", artifacts_dir)
    monkeypatch.setattr(services, "ARTIFACTS_INDEX_PATH", artifacts_dir / "records.json")

    created = _artifact(artifact_id="mutated")

    def mutate(records: list[ArtifactRecord]) -> ArtifactRecord:
        records.append(created)
        return created

    result = services._mutate_records(mutate)

    assert result.artifact_id == "mutated"
    records = services._load_records()
    assert [record.artifact_id for record in records] == ["mutated"]


def test_find_existing_artifact_ignores_record_with_overwide_coverage() -> None:
    request_scope = ArtifactRequestScope(
        start="2026-01-01",
        end="2026-02-10",
        bbox=(1.0, 2.0, 3.0, 4.0),
    )
    stale_artifact = _artifact(artifact_id="stale", end="2026-02-29")
    stale_artifact.request_scope = request_scope
    valid_artifact = _artifact(artifact_id="valid", end="2026-02-10")
    valid_artifact.request_scope = request_scope

    result = services._find_existing_artifact_in_records(
        records=[stale_artifact, valid_artifact],
        dataset_id="chirps3_precipitation_daily",
        request_scope=request_scope,
    )

    assert result == valid_artifact


def test_find_existing_artifact_ignores_stale_record(monkeypatch: pytest.MonkeyPatch) -> None:
    request_scope = ArtifactRequestScope(
        start="2026-01-01",
        end="2026-02-10",
        bbox=(1.0, 2.0, 3.0, 4.0),
    )
    stale_artifact = _artifact(artifact_id="stale", end="2026-02-10")
    valid_artifact = _artifact(artifact_id="valid", end="2026-02-10")
    monkeypatch.setattr(
        services,
        "_artifact_storage_exists",
        lambda record: record.artifact_id != "stale",
    )

    result = services._find_existing_artifact_in_records(
        records=[stale_artifact, valid_artifact],
        dataset_id="chirps3_precipitation_daily",
        request_scope=request_scope,
    )

    assert result == valid_artifact


def test_list_datasets_ignores_stale_records(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _artifact(artifact_id="stale", created_at="2026-01-10T00:00:00+00:00", end="2026-01-10"),
        _artifact(artifact_id="valid", created_at="2026-01-11T00:00:00+00:00", end="2026-01-11"),
    ]
    monkeypatch.setattr(services, "_load_records", lambda: records)
    monkeypatch.setattr(
        services,
        "_artifact_storage_exists",
        lambda record: record.artifact_id != "stale",
    )
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "short_name": "CHIRPS3 precip",
            "period_type": "daily",
            "units": "mm",
            "resolution": "5 km x 5 km",
            "source": "CHIRPS v3",
            "source_url": "https://example.com/chirps",
        },
    )

    result = services.list_datasets()

    assert len(result.items) == 1
    assert result.items[0].extent.temporal.end == "2026-01-11"


def test_temporal_coverage_matches_request_scope_allows_open_ended_reuse() -> None:
    request_scope = ArtifactRequestScope(
        start="2026-01-01",
        end=None,
        bbox=(1.0, 2.0, 3.0, 4.0),
    )

    assert services._temporal_coverage_matches_request_scope(
        CoverageTemporal(start="2026-01-01", end="2026-02-10"),
        request_scope,
    )


def test_create_artifact_rejects_partial_download_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }

    with pytest.raises(services.HTTPException) as exc_info:
        services.create_artifact(
            dataset=dataset,
            start="2026-01-01",
            end="2026-02-10",
            download_start=None,
            download_end="2026-02-10",
            bbox=[1.0, 2.0, 3.0, 4.0],
            overwrite=False,
            publish=False,
        )

    assert exc_info.value.status_code == 400
    assert "download_start and download_end must either both be provided or both be omitted" in str(
        exc_info.value.detail
    )


def test_create_artifact_rejects_download_scope_outside_request_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }

    with pytest.raises(services.HTTPException) as exc_info:
        services.create_artifact(
            dataset=dataset,
            start="2026-01-01",
            end="2026-02-10",
            download_start="2026-02-01",
            download_end="2026-02-11",
            bbox=[1.0, 2.0, 3.0, 4.0],
            overwrite=False,
            publish=False,
        )

    assert exc_info.value.status_code == 400
    assert "download_end must be less than or equal to end" in str(exc_info.value.detail)


def _icechunk_artifact(
    *,
    artifact_id: str = "ic1",
    path: str = "/tmp/test.icechunk",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ICECHUNK,
        asset_paths=[path],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end="2026-01-10",
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-10"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime(2026, 1, 10, tzinfo=UTC),
        publication=ArtifactPublication(status=PublicationStatus.PUBLISHED),
    )


def test_get_zarr_store_info_dispatches_to_icechunk_for_icechunk_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "test.icechunk"
    store_path.mkdir()
    artifact = _icechunk_artifact(path=str(store_path))

    monkeypatch.setattr(
        services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: artifact,
    )

    called_with: list[str] = []

    def fake_icechunk_store_info(dataset_id: str, art: ArtifactRecord) -> dict:
        called_with.append(dataset_id)
        return {"kind": "ZarrListing", "dataset_id": dataset_id, "format": art.format, "entries": []}

    monkeypatch.setattr(services, "_icechunk_store_info", fake_icechunk_store_info)

    result = services.get_dataset_zarr_store_info_or_404("chirps3_precipitation_daily")

    assert result["kind"] == "ZarrListing"
    assert called_with == ["chirps3_precipitation_daily"]


def test_get_zarr_store_file_dispatches_to_icechunk_for_icechunk_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "test.icechunk"
    store_path.mkdir()
    artifact = _icechunk_artifact(path=str(store_path))

    monkeypatch.setattr(
        services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: artifact,
    )

    served_keys: list[str] = []

    from starlette.responses import Response

    def fake_serve_icechunk_key(dataset_id: str, art: ArtifactRecord, relative_path: str) -> Response:
        served_keys.append(relative_path)
        return Response(content=b'{"zarr_format": 3}', media_type="application/json")

    monkeypatch.setattr(services, "_serve_icechunk_key", fake_serve_icechunk_key)

    services.get_dataset_zarr_store_file_or_404("chirps3_precipitation_daily", "t2m/zarr.json")

    assert served_keys == ["t2m/zarr.json"]
