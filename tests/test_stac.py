from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pystac
import pytest
from fastapi.testclient import TestClient

from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    PublicationStatus,
)
from climate_api.stac import services as stac_services


def _artifact(
    *,
    artifact_id: str,
    dataset_id: str = "chirps3_precipitation_daily",
    dataset_name: str = "CHIRPS3 precipitation",
    variable: str = "precip",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    status: PublicationStatus = PublicationStatus.PUBLISHED,
    format: ArtifactFormat = ArtifactFormat.ZARR,
    path: str | None = "/tmp/chirps3_precipitation_daily.zarr",
    asset_paths: list[str] | None = None,
    temporal_start: str = "2026-01-01",
    temporal_end: str = "2026-01-10",
    created_at: datetime | None = None,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        variable=variable,
        format=format,
        path=path,
        asset_paths=[path] if asset_paths is None and path is not None else (asset_paths or []),
        variables=[variable],
        request_scope=ArtifactRequestScope(
            start=temporal_start,
            end=temporal_end,
            extent_id="sle",
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start=temporal_start, end=temporal_end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=created_at or datetime(2026, 1, 10, tzinfo=UTC),
        publication=ArtifactPublication(
            status=status,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def test_catalog_lists_published_zarr_datasets(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Catalog"
    assert payload["links"][0]["href"].endswith("/stac/catalog.json")
    assert any(link["rel"] == "child" for link in payload["links"])


def test_catalog_self_link_reflects_request_path(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )

    response = client.get("/stac")

    assert response.status_code == 200
    payload = response.json()
    assert payload["links"][0]["href"].endswith("/stac")


def test_catalog_excludes_unpublished_and_netcdf(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(
            items=[
                _artifact(artifact_id="a1", status=PublicationStatus.UNPUBLISHED),
                _artifact(artifact_id="a2", format=ArtifactFormat.NETCDF),
            ]
        ),
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    assert [link for link in payload["links"] if link["rel"] == "child"] == []


def test_collection_uses_xstac_and_adds_expected_fields(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "units": "mm", "source": "CHIRPS v3"},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily_sle",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {
                "x": {
                    "type": "spatial",
                    "axis": "x",
                    "extent": [1.0, 3.0],
                    "step": 0.05000000074505806,
                    "reference_system": 4326,
                },
                "y": {
                    "type": "spatial",
                    "axis": "y",
                    "extent": [2.0, 4.0],
                    "step": -0.05000000074505806,
                    "reference_system": 4326,
                },
                "time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]},
            },
            "stac_extensions": ["https://stac-extensions.github.io/projection/v2.0.0/schema.json"],
            "cube:variables": {
                "precip": {
                    "type": "data",
                    "dimensions": ["time", "y", "x"],
                    "attrs": {
                        "long_name": "Precipitation",
                        "units": "mm/day",
                        "TIFFTAG_SOFTWARE": "IDL 9.0.0",
                    },
                },
            },
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(
        stac_services,
        "_zarr_asset_metadata",
        lambda _: {"zarr:consolidated": True, "zarr:zarr_format": 3},
    )
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Collection"
    assert payload["description"] == "Published GeoZarr dataset for CHIRPS3 precipitation"
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily_sle")
    assert payload["assets"]["zarr"]["xarray:open_kwargs"] == {"consolidated": True}
    assert payload["extent"]["temporal"]["interval"] == [["2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z"]]
    assert payload["cube:dimensions"]["x"]["step"] == 0.05
    assert payload["cube:dimensions"]["y"]["step"] == -0.05
    assert payload["cube:dimensions"]["time"]["extent"] == ["2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z"]
    assert payload["cube:dimensions"]["time"]["step"] == "P1D"
    assert payload["cube:variables"]["precip"]["unit"] == "mm/day"
    assert payload["cube:variables"]["precip"]["attrs"] == {
        "long_name": "Precipitation",
        "units": "mm/day",
    }
    assert "https://stac-extensions.github.io/projection/v2.0.0/schema.json" in payload["stac_extensions"]


def test_collection_uses_configured_base_url(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_API_BASE_URL", "https://climate.example.org")
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "units": "mm"},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily_sle",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["links"][0]["href"] == "https://climate.example.org/stac/collections/chirps3_precipitation_daily_sle"


def test_collection_sets_hourly_step_to_pt1h(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    hourly_artifact = _artifact(
        artifact_id="a1",
        dataset_id="era5land_temperature_hourly",
        dataset_name="ERA5-Land temperature",
        variable="t2m",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path="/tmp/era5land_temperature_hourly.zarr",
        temporal_start="2026-01-01T00",
        temporal_end="2026-01-01T12",
    )
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[hourly_artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "hourly", "source": "ERA5-Land", "short_name": "2m temperature"},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "era5land_temperature_hourly_sle",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {
                "valid_time": {"type": "temporal", "extent": ["2026-01-01T00", "2026-01-01T12"]},
            },
            "cube:variables": {"t2m": {"type": "data", "dimensions": ["valid_time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/stac/collections/era5land_temperature_hourly_sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cube:dimensions"]["valid_time"]["step"] == "PT1H"


def test_collection_uses_level0_href_for_multiscale_store(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    artifact = _artifact(artifact_id="a1", path=str(tmp_path / "chirps3_precipitation_daily.zarr"))
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "source": "CHIRPS v3", "cache_info": {"multiscales": {"levels": 2}}},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily_sle",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": None})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily_sle/0")


def test_collection_uses_level0_href_for_remote_multiscale_store(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(artifact_id="a1", path="s3://example-bucket/chirps3_precipitation_daily.zarr")
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "source": "CHIRPS v3", "cache_info": {"multiscales": {"levels": 2}}},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily_sle",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": None})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily_sle/0")


def test_collection_returns_404_for_unknown_dataset(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[]),
    )

    response = client.get("/stac/collections/unknown-dataset")

    assert response.status_code == 404


def test_catalog_prefers_latest_artifact_per_managed_dataset(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    older = _artifact(artifact_id="a1", created_at=datetime(2026, 1, 9, tzinfo=UTC))
    newer = _artifact(artifact_id="a2", created_at=datetime(2026, 1, 10, tzinfo=UTC))
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[older, newer]),
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    child_links = [link for link in payload["links"] if link["rel"] == "child"]
    assert len(child_links) == 1


def test_catalog_sorts_child_links_by_managed_dataset_id(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(
            items=[
                _artifact(artifact_id="a1", managed_dataset_id="worldpop_population_yearly_sle"),
                _artifact(
                    artifact_id="a2",
                    dataset_id="aa_dataset",
                    dataset_name="AA Dataset",
                    variable="value",
                    managed_dataset_id="aa_dataset_sle",
                    path="/tmp/aa_dataset.zarr",
                ),
            ]
        ),
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    child_hrefs = [link["href"] for link in payload["links"] if link["rel"] == "child"]
    assert child_hrefs == sorted(child_hrefs)


def test_build_collection_with_xstac_normalizes_pystac_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyDataset:
        def close(self) -> None:
            pass

    artifact = _artifact(artifact_id="a1")
    template = pystac.Collection(
        id="chirps3_precipitation_daily_sle",
        description="template",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[1.0, 2.0, 3.0, 4.0]]),
            temporal=pystac.TemporalExtent([[datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 10, tzinfo=UTC)]]),
        ),
        title="CHIRPS3 precipitation",
        license="proprietary",
    )
    template.add_asset("zarr", pystac.Asset(href="http://example.test/zarr"))
    monkeypatch.setattr(stac_services, "open_zarr_dataset", lambda _: DummyDataset())
    monkeypatch.setattr(stac_services, "get_lon_lat_dims", lambda _: ("x", "y"))
    monkeypatch.setattr(stac_services, "get_time_dim", lambda _: "time")
    monkeypatch.setattr(stac_services, "xarray_to_stac", lambda *args, **kwargs: template)

    payload = stac_services._build_collection_with_xstac(artifact=artifact, template=template)

    assert isinstance(payload, dict)
    assert payload["type"] == "Collection"
    assert payload["id"] == "chirps3_precipitation_daily_sle"


def test_zarr_consolidated_flag_detects_v3_and_v2_markers(tmp_path: Path) -> None:
    v3_consolidated = tmp_path / "v3_consolidated.zarr"
    v3_consolidated.mkdir()
    (v3_consolidated / "zarr.json").write_text('{"consolidated_metadata": {}}', encoding="utf-8")

    v3_unconsolidated = tmp_path / "v3_unconsolidated.zarr"
    v3_unconsolidated.mkdir()
    (v3_unconsolidated / "zarr.json").write_text("{}", encoding="utf-8")

    v2_consolidated = tmp_path / "v2_consolidated.zarr"
    v2_consolidated.mkdir()
    (v2_consolidated / ".zmetadata").write_text("{}", encoding="utf-8")

    assert stac_services._zarr_consolidated_flag(str(v3_consolidated)) is True
    assert stac_services._zarr_consolidated_flag(str(v3_unconsolidated)) is False
    assert stac_services._zarr_consolidated_flag(str(v2_consolidated)) is True
    assert stac_services._zarr_consolidated_flag("s3://example-bucket/store.zarr") is None


def test_collection_preserves_template_links_when_xstac_mutates_template(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyDataset:
        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(stac_services, "open_zarr_dataset", lambda _: DummyDataset())
    monkeypatch.setattr(stac_services, "get_lon_lat_dims", lambda _: ("x", "y"))
    monkeypatch.setattr(stac_services, "get_time_dim", lambda _: "time")

    def fake_xarray_to_stac(*args: object, **kwargs: object) -> pystac.Collection:
        template = kwargs["template"] if "template" in kwargs else args[1]
        assert isinstance(template, pystac.Collection)
        template.extra_fields["cube:dimensions"] = {
            "time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}
        }
        template.extra_fields["cube:variables"] = {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}}
        return template

    monkeypatch.setattr(stac_services, "xarray_to_stac", fake_xarray_to_stac)
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 200
    payload = response.json()
    links = {link["rel"]: link["href"] for link in payload["links"]}
    assert links["self"].endswith("/stac/collections/chirps3_precipitation_daily_sle")
    assert links["root"].endswith("/stac/catalog.json")
    assert links["parent"].endswith("/stac/catalog.json")
    assert links["alternate"].endswith("/datasets/chirps3_precipitation_daily_sle")


def test_collection_returns_500_for_missing_artifact_store_metadata(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(artifact_id="a1", path=None, asset_paths=[])
    monkeypatch.setattr(ingestion_services, "list_artifacts", lambda: SimpleNamespace(items=[artifact]))
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 500
    assert "no readable storage path metadata" in response.json()["detail"]


def test_collection_returns_503_when_zarr_store_cannot_be_opened(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def raise_file_not_found(_: str) -> None:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(
        stac_services,
        "open_zarr_dataset",
        raise_file_not_found,
    )

    response = client.get("/stac/collections/chirps3_precipitation_daily_sle")

    assert response.status_code == 503
    assert "temporarily unavailable" in response.json()["detail"]
