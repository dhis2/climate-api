from pydantic import BaseModel


class SpatialAggregationRunRequest(BaseModel):
    """Execute spatial aggregation component from cached dataset."""

    dataset_id: str
    start: str
    end: str
    feature_source: FeatureSourceConfig
    method: AggregationMethod = AggregationMethod.MEAN
    bbox: list[float] | None = None
    feature_id_property: str = "id"
    max_preview_rows: int = 20


class SpatialAggregationRunResponse(BaseModel):
    """Spatial aggregation result with sample rows."""

    dataset_id: str
    record_count: int
    preview: list[dict[str, Any]]