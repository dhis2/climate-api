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


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path="/tmp/chirps3_precipitation_daily.zarr",
        asset_paths=["/tmp/chirps3_precipitation_daily.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
            extent_id="sle",
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
    assert dataset.dataset_id == "chirps3_precipitation_daily_sle"
    assert dataset.source_dataset_id == "chirps3_precipitation_daily"
    assert dataset.period_type == "daily"
    assert dataset.units == "mm"
    assert dataset.extent.temporal.end == "2026-01-11"
    assert dataset.publication.status == PublicationStatus.PUBLISHED
    assert any(link.href == f"/zarr/{dataset.dataset_id}" for link in dataset.links)


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
    assert result.items[0].dataset.dataset_id == "chirps3_precipitation_daily_sle"


def test_managed_dataset_id_prefers_extent_id_when_present() -> None:
    artifact = _artifact(artifact_id="a1")

    assert managed_dataset_id_for(artifact) == "chirps3_precipitation_daily_sle"


def test_find_existing_artifact_ignores_record_with_overwide_coverage() -> None:
    request_scope = ArtifactRequestScope(
        start="2026-01-01",
        end="2026-02-10",
        extent_id="sle",
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
        prefer_zarr=True,
    )

    assert result == valid_artifact


def test_create_artifact_computes_coverage_from_created_artifact_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dataset: dict[str, object] = {
        "id": "worldpop_population_yearly",
        "name": "Total population (WorldPop Global12)",
        "variable": "pop_total",
        "period_type": "yearly",
    }
    created_file = tmp_path / "worldpop_population_yearly_2020.nc"
    created_file.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr(services.downloader, "download_dataset", lambda *_, **__: [created_file])
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: None)
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)

    captured: dict[str, object] = {}

    def fake_get_data_coverage_for_paths(
        dataset_arg: dict[str, object],
        *,
        zarr_path: str | None = None,
        netcdf_paths: list[str] | None = None,
    ) -> dict[str, object]:
        captured["dataset_id"] = dataset_arg["id"]
        captured["zarr_path"] = zarr_path
        captured["netcdf_paths"] = netcdf_paths
        return {
            "coverage": {
                "temporal": {"start": "2020", "end": "2020"},
                "spatial": {"xmin": -13.3, "ymin": 6.9, "xmax": -10.2, "ymax": 10.0},
            }
        }

    monkeypatch.setattr(services, "get_data_coverage_for_paths", fake_get_data_coverage_for_paths)
    monkeypatch.setattr(services, "_store_artifact_record", lambda record, **_: record)

    artifact = services.create_artifact(
        dataset=dataset,
        start="2020",
        end="2020",
        extent_id="sle",
        bbox=[-13.5, 6.9, -10.1, 10.0],
        country_code="SLE",
        overwrite=False,
        prefer_zarr=False,
        publish=False,
    )

    assert captured["dataset_id"] == "worldpop_population_yearly"
    assert captured["zarr_path"] is None
    assert captured["netcdf_paths"] == [str(created_file.resolve())]
    assert artifact.coverage.temporal.start == "2020"
    assert artifact.coverage.temporal.end == "2020"


def test_create_artifact_normalizes_request_scope_to_dataset_period(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dataset: dict[str, object] = {
        "id": "era5land_temperature_hourly",
        "name": "2m temperature (ERA5-Land)",
        "variable": "t2m",
        "period_type": "hourly",
    }
    created_file = tmp_path / "era5land_temperature_hourly_2026-04-21.nc"
    created_file.write_text("dummy", encoding="utf-8")

    captured_download: dict[str, object] = {}

    def fake_download_dataset(
        dataset_arg: dict[str, object],
        *,
        start: str,
        end: str | None,
        **_: object,
    ) -> list[Path]:
        captured_download["dataset_id"] = dataset_arg["id"]
        captured_download["start"] = start
        captured_download["end"] = end
        return [created_file]

    monkeypatch.setattr(services.downloader, "download_dataset", fake_download_dataset)
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: None)
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)
    monkeypatch.setattr(
        services,
        "get_data_coverage_for_paths",
        lambda *_, **__: {
            "coverage": {
                "temporal": {"start": "2026-04-21T12", "end": "2026-04-21T13"},
                "spatial": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0},
            }
        },
    )
    monkeypatch.setattr(services, "_store_artifact_record", lambda record, **_: record)

    artifact = services.create_artifact(
        dataset=dataset,
        start="2026-04-21T12:15:00",
        end="2026-04-21T13:45:00",
        extent_id="sle",
        bbox=[1.0, 2.0, 3.0, 4.0],
        country_code=None,
        overwrite=False,
        prefer_zarr=False,
        publish=False,
    )

    assert captured_download == {
        "dataset_id": "era5land_temperature_hourly",
        "start": "2026-04-21T12",
        "end": "2026-04-21T13",
    }
    assert artifact.request_scope.start == "2026-04-21T12"
    assert artifact.request_scope.end == "2026-04-21T13"


def test_create_artifact_returns_409_when_downloaded_artifact_has_no_data(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    dataset: dict[str, object] = {
        "id": "worldpop_population_yearly",
        "name": "Total population (WorldPop Global12)",
        "variable": "pop_total",
        "period_type": "yearly",
    }
    created_file = tmp_path / "worldpop_population_yearly_2020.nc"
    created_file.write_text("dummy", encoding="utf-8")

    monkeypatch.setattr(services.downloader, "download_dataset", lambda *_, **__: [created_file])
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: None)
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)
    monkeypatch.setattr(
        services,
        "get_data_coverage_for_paths",
        lambda *_, **__: {
            "has_data": False,
            "coverage": {
                "temporal": {"start": None, "end": None},
                "spatial": {"xmin": None, "ymin": None, "xmax": None, "ymax": None},
            },
        },
    )

    with pytest.raises(services.HTTPException) as exc_info:
        services.create_artifact(
            dataset=dataset,
            start="2020",
            end="2020",
            extent_id="sle",
            bbox=[-13.5, 6.9, -10.1, 10.0],
            country_code="SLE",
            overwrite=False,
            prefer_zarr=False,
            publish=False,
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Downloaded artifact contains no data for the requested scope"


def test_create_artifact_can_download_delta_while_recording_full_request_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }
    created_file = tmp_path / "chirps3_precipitation_daily_2026-02-01_2026-02-10.nc"
    created_file.write_text("dummy", encoding="utf-8")

    captured_download: dict[str, object] = {}

    def fake_download_dataset(
        dataset_arg: dict[str, object],
        *,
        start: str,
        end: str | None,
        **_: object,
    ) -> list[Path]:
        captured_download["dataset_id"] = dataset_arg["id"]
        captured_download["start"] = start
        captured_download["end"] = end
        return [created_file]

    monkeypatch.setattr(services.downloader, "download_dataset", fake_download_dataset)
    monkeypatch.setattr(services.downloader, "build_dataset_zarr", lambda *_, **__: None)
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: tmp_path / "chirps3_precipitation_daily.zarr")
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)
    monkeypatch.setattr(
        services,
        "get_data_coverage_for_paths",
        lambda *_, **__: {
            "coverage": {
                "temporal": {"start": "2026-01-01", "end": "2026-02-10"},
                "spatial": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0},
            }
        },
    )
    monkeypatch.setattr(services, "_store_artifact_record", lambda record, **_: record)

    artifact = services.create_artifact(
        dataset=dataset,
        start="2026-01-01",
        end="2026-02-10",
        download_start="2026-02-01",
        download_end="2026-02-10",
        extent_id="sle",
        bbox=[1.0, 2.0, 3.0, 4.0],
        country_code=None,
        overwrite=False,
        prefer_zarr=True,
        publish=False,
    )

    assert captured_download == {
        "dataset_id": "chirps3_precipitation_daily",
        "start": "2026-02-01",
        "end": "2026-02-10",
    }
    assert artifact.request_scope.start == "2026-01-01"
    assert artifact.request_scope.end == "2026-02-10"
    assert artifact.coverage.temporal.start == "2026-01-01"
    assert artifact.coverage.temporal.end == "2026-02-10"


def test_create_artifact_delta_requires_canonical_zarr_when_prefer_zarr_is_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }
    created_file = tmp_path / "chirps3_precipitation_daily_2026-02-01_2026-02-10.nc"
    created_file.write_text("dummy", encoding="utf-8")
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"

    captured_build: dict[str, object] = {}

    def fake_build_dataset_zarr(dataset_arg: dict[str, object], *, start: str | None, end: str | None) -> None:
        captured_build["dataset_id"] = dataset_arg["id"]
        captured_build["start"] = start
        captured_build["end"] = end

    monkeypatch.setattr(services.downloader, "download_dataset", lambda *_, **__: [created_file])
    monkeypatch.setattr(services.downloader, "build_dataset_zarr", fake_build_dataset_zarr)
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: zarr_path)
    monkeypatch.setattr(services.downloader, "get_cache_files", lambda _: [created_file])
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)
    monkeypatch.setattr(
        services,
        "get_data_coverage_for_paths",
        lambda *_, **__: {
            "coverage": {
                "temporal": {"start": "2026-01-01", "end": "2026-02-10"},
                "spatial": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0},
            }
        },
    )
    monkeypatch.setattr(services, "_store_artifact_record", lambda record, **_: record)

    artifact = services.create_artifact(
        dataset=dataset,
        start="2026-01-01",
        end="2026-02-10",
        download_start="2026-02-01",
        download_end="2026-02-10",
        extent_id="sle",
        bbox=[1.0, 2.0, 3.0, 4.0],
        country_code=None,
        overwrite=False,
        prefer_zarr=False,
        publish=False,
    )

    assert captured_build == {
        "dataset_id": "chirps3_precipitation_daily",
        "start": "2026-01-01",
        "end": "2026-02-10",
    }
    assert artifact.format == ArtifactFormat.ZARR


def test_create_artifact_delta_fails_when_canonical_zarr_build_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }
    created_file = tmp_path / "chirps3_precipitation_daily_2026-02-01_2026-02-10.nc"
    created_file.write_text("dummy", encoding="utf-8")

    def fail_build_dataset_zarr(*_: object, **__: object) -> None:
        raise ValueError("zarr failed")

    monkeypatch.setattr(services.downloader, "download_dataset", lambda *_, **__: [created_file])
    monkeypatch.setattr(services.downloader, "build_dataset_zarr", fail_build_dataset_zarr)
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: None)
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)

    with pytest.raises(services.HTTPException) as exc_info:
        services.create_artifact(
            dataset=dataset,
            start="2026-01-01",
            end="2026-02-10",
            download_start="2026-02-01",
            download_end="2026-02-10",
            extent_id="sle",
            bbox=[1.0, 2.0, 3.0, 4.0],
            country_code=None,
            overwrite=False,
            prefer_zarr=True,
            publish=False,
        )

    assert exc_info.value.status_code == 500
    assert "Append sync requires Zarr materialization" in str(exc_info.value.detail)


def test_create_artifact_delta_rejects_short_rebuilt_coverage(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dataset: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "name": "Total precipitation (CHIRPS3)",
        "variable": "precip",
        "period_type": "daily",
    }
    created_file = tmp_path / "chirps3_precipitation_daily_2026-02-01_2026-02-10.nc"
    created_file.write_text("dummy", encoding="utf-8")
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"

    monkeypatch.setattr(services.downloader, "download_dataset", lambda *_, **__: [created_file])
    monkeypatch.setattr(services.downloader, "build_dataset_zarr", lambda *_, **__: None)
    monkeypatch.setattr(services.downloader, "get_zarr_path", lambda _: zarr_path)
    monkeypatch.setattr(services.downloader, "get_cache_files", lambda _: [created_file])
    monkeypatch.setattr(services, "_find_existing_artifact", lambda **_: None)
    monkeypatch.setattr(
        services,
        "get_data_coverage_for_paths",
        lambda *_, **__: {
            "coverage": {
                "temporal": {"start": "2026-02-01", "end": "2026-02-10"},
                "spatial": {"xmin": 1.0, "ymin": 2.0, "xmax": 3.0, "ymax": 4.0},
            }
        },
    )

    with pytest.raises(services.HTTPException) as exc_info:
        services.create_artifact(
            dataset=dataset,
            start="2026-01-01",
            end="2026-02-10",
            download_start="2026-02-01",
            download_end="2026-02-10",
            extent_id="sle",
            bbox=[1.0, 2.0, 3.0, 4.0],
            country_code=None,
            overwrite=False,
            prefer_zarr=True,
            publish=False,
        )

    assert exc_info.value.status_code == 409
    assert "coverage=2026-02-01..2026-02-10" in str(exc_info.value.detail)
    assert "request=2026-01-01..2026-02-10" in str(exc_info.value.detail)
