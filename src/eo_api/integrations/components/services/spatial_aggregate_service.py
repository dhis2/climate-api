"""Generic spatial aggregation services for gridded datasets."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from pygeoapi.process.base import ProcessorExecuteError
from rasterio.mask import mask
from rasterio.warp import transform_geom
from rioxarray.exceptions import NoDataInBounds

from eo_api.integrations.components.services.dhis2_datavalues_service import build_data_value_set

_YEAR_RE = re.compile(r"(19|20)\d{2}")


def resolve_value_var(dataset: xr.Dataset, preferred_var: str | None = None) -> str:
    """Resolve the value variable from a dataset."""
    if preferred_var and preferred_var in dataset.data_vars:
        return preferred_var
    keys = list(dataset.data_vars.keys())
    if not keys:
        raise ProcessorExecuteError("Dataset has no data variables")
    return str(keys[0])


def clip_spatial_series(
    data_array: xr.DataArray,
    geometry: dict[str, Any],
    spatial_reducer: str,
    *,
    time_dim: str = "time",
) -> pd.Series:
    """Clip raster by geometry and reduce over spatial dims to a time series."""
    try:
        clipped = data_array.rio.clip([geometry], crs="EPSG:4326", drop=True)
    except NoDataInBounds:
        return pd.Series(dtype=float)
    spatial_dims = [dim for dim in clipped.dims if dim != time_dim]
    if not spatial_dims:
        raise ProcessorExecuteError("Unable to resolve spatial dimensions in dataset")
    reduced = (
        clipped.mean(dim=spatial_dims, skipna=True)
        if spatial_reducer == "mean"
        else clipped.sum(dim=spatial_dims, skipna=True)
    )
    series: pd.Series[Any] = reduced.to_series().dropna()
    if series.empty:
        return series
    series.index = pd.DatetimeIndex(pd.to_datetime(series.index))
    return series


def _extract_year(path: str, fallback_year: int) -> int:
    match = _YEAR_RE.search(Path(path).name)
    if match:
        return int(match.group(0))
    return fallback_year


def _compute_stat(values: np.ndarray[Any, np.dtype[np.float64]], reducer: str) -> float | None:
    if values.size == 0:
        return None
    if reducer == "sum":
        return float(np.sum(values))
    if reducer == "mean":
        return float(np.mean(values))
    raise ValueError(f"Unsupported reducer '{reducer}'")


def build_worldpop_rows(
    *,
    files: list[str],
    feature_collection: dict[str, Any],
    start_year: int,
    org_unit_id_property: str = "id",
    reducer: str = "sum",
) -> dict[str, Any]:
    """Aggregate WorldPop raster files by features and return canonical rows."""
    if feature_collection.get("type") != "FeatureCollection":
        raise ValueError("feature_collection must be a GeoJSON FeatureCollection")
    features = feature_collection.get("features")
    if not isinstance(features, list):
        raise ValueError("feature_collection.features must be an array")
    if reducer not in {"sum", "mean"}:
        raise ValueError("reducer must be one of: sum, mean")

    rows: list[dict[str, Any]] = []
    yearly: list[dict[str, Any]] = []
    for file_path in files:
        year = _extract_year(file_path, start_year)
        period = f"{year:04d}"
        file_row_count = 0

        with rasterio.open(file_path) as src:
            raster_crs = src.crs
            nodata = src.nodata

            for index, feature in enumerate(features):
                if not isinstance(feature, dict):
                    continue
                geometry = feature.get("geometry")
                if not isinstance(geometry, dict):
                    continue
                props = feature.get("properties")
                properties = props if isinstance(props, dict) else {}
                org_unit = feature.get("id") or properties.get(org_unit_id_property) or properties.get("id")
                if org_unit is None:
                    continue

                projected_geometry = geometry
                if raster_crs:
                    projected_geometry = transform_geom("EPSG:4326", raster_crs, geometry, precision=12)

                try:
                    raster_data, _ = mask(
                        src,
                        [projected_geometry],
                        indexes=1,
                        crop=True,
                        all_touched=False,
                        filled=False,
                    )
                except ValueError:
                    raster_data = np.ma.array([], dtype=np.float64)

                values: np.ndarray[Any, np.dtype[np.float64]]
                if isinstance(raster_data, np.ma.MaskedArray):
                    values = raster_data.compressed().astype(np.float64)
                else:
                    values = np.array(raster_data, dtype=np.float64).ravel()
                if values.size and nodata is not None:
                    values = values[~np.isclose(values, float(nodata), equal_nan=True)]

                stat_value = _compute_stat(values, reducer=reducer)
                if stat_value is None:
                    continue
                rows.append({"orgUnit": str(org_unit), "period": period, "value": stat_value, "featureIndex": index})
                file_row_count += 1

        yearly.append({"year": year, "row_count": file_row_count, "raster_path": file_path, "reducer": reducer})

    return {"rows": rows, "summary": {"years_processed": sorted({item["year"] for item in yearly}), "yearly": yearly}}


def build_worldpop_datavalueset(
    *,
    features_geojson: dict[str, Any],
    raster_path: str,
    year: int,
    data_element: str,
    org_unit_id_property: str = "id",
    reducer: str = "sum",
    category_option_combo: str | None = None,
    attribute_option_combo: str | None = None,
    data_set: str | None = None,
) -> dict[str, Any]:
    """Aggregate one raster by features and format a DHIS2-compatible payload."""
    features = features_geojson.get("features")
    if not isinstance(features, list):
        raise ValueError("features_geojson.features must be an array")
    rows_result = build_worldpop_rows(
        files=[raster_path],
        feature_collection=features_geojson,
        start_year=year,
        org_unit_id_property=org_unit_id_property,
        reducer=reducer,
    )
    rows = cast(list[dict[str, Any]], rows_result["rows"])
    payload = build_data_value_set(
        rows=rows,
        data_element=data_element,
        category_option_combo=category_option_combo,
        attribute_option_combo=attribute_option_combo,
        data_set=data_set,
    )
    return {
        "dataValueSet": payload["dataValueSet"],
        "table": payload["table"],
        "summary": {
            "year": year,
            "reducer": reducer,
            "feature_count": len(features),
            "row_count": len(rows),
            "raster_path": raster_path,
        },
    }
