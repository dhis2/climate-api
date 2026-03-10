from typing import Any

from eo_api.integrations.components.services import gridded_aggregation_service as service


def test_temporal_stage_exits_on_invalid_disaggregation() -> None:
    result = service.run_temporal_aggregation_stage(
        params={
            "source_temporal_resolution": "yearly",
            "temporal_resolution": "monthly",
            "files": ["/tmp/fake.tif"],
        },
        source_resolution_default="yearly",
        supported_resolutions={"yearly", "annual"},
        supports_downsample=True,
    )
    assert result["_step_control"]["action"] == "exit"
    assert result["_step_control"]["reason"]


def test_temporal_stage_passes_through_when_no_aggregation_inputs() -> None:
    result = service.run_temporal_aggregation_stage(
        params={
            "source_temporal_resolution": "yearly",
            "temporal_resolution": "yearly",
            "files": ["/tmp/fake.tif"],
        },
        source_resolution_default="yearly",
        supported_resolutions={"yearly", "annual"},
        supports_downsample=True,
    )
    assert result["files"] == ["/tmp/fake.tif"]
    assert result["_step_control"]["action"] == "pass_through"


def test_spatial_stage_passes_through_existing_rows() -> None:
    rows = [{"orgUnit": "OU_1", "period": "202601", "value": 1.2}]
    result = service.run_spatial_aggregation_stage({"rows": rows})
    assert result["rows"] == rows
    assert result["_step_control"]["action"] == "pass_through"


def test_spatial_stage_aggregates_raster_files(monkeypatch: Any) -> None:
    def _fake_aggregate_gridded_rows_by_features(**_: object) -> dict[str, Any]:
        return {"rows": [{"orgUnit": "OU_1", "period": "2026", "value": 123.0}], "summary": {"years_processed": [2026]}}

    monkeypatch.setattr(service, "aggregate_gridded_rows_by_features", _fake_aggregate_gridded_rows_by_features)
    result = service.run_spatial_aggregation_stage(
        {
            "files": ["/tmp/worldpop_2026.tif"],
            "feature_collection": {"type": "FeatureCollection", "features": []},
            "start_year": 2026,
            "org_unit_id_property": "id",
            "reducer": "sum",
        }
    )
    assert result["rows"][0]["orgUnit"] == "OU_1"
    assert result["summary"]["years_processed"] == [2026]
