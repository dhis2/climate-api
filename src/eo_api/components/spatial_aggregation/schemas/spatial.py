from pydantic import BaseModel


class AggregationMethod(StrEnum):
    """Supported numeric aggregation methods."""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class SpatialAggregationConfig(BaseModel):
    """Spatial aggregation config."""

    method: AggregationMethod = AggregationMethod.MEAN
