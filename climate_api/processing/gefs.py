"""Built-in derivation pipeline for GEFS-like forecast stores from dynamical.org.

This function is the default ``ingestion.function`` for derived GEFS dataset
templates.  Plugin authors can reference it directly, extend it, or replace it
with their own function following the same contract:

    def my_derive_fn(
        *,
        store_config: dict[str, Any],
        output_path: Path,
        extent: dict[str, Any] | None,
        variable: str,
        period_type: str,
    ) -> None: ...

The function must write a valid consolidated Zarr v3 store at ``output_path``
with dimensions ``(time, latitude, longitude)`` in ascending coordinate order.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    import xarray as xr

logger = logging.getLogger(__name__)


def most_recent_complete_init(ds_raw: xr.Dataset, *, variable: str) -> pd.Timestamp:
    """Return the most recent init_time that has a complete forecast.

    The latest run is often still being distributed and has NaN placeholders
    at the longer lead_times.  Walk backwards through the last 5 init_times
    and return the first one whose final lead_time is non-NaN.
    """
    import numpy as np
    import pandas as pd

    init_times = sorted(ds_raw.init_time.values, reverse=True)
    for candidate in init_times[:5]:
        t = pd.Timestamp(candidate)
        sel_kwargs: dict[str, object] = {"init_time": t, "lead_time": ds_raw.lead_time.values[-1]}
        if "ensemble_member" in ds_raw[variable].dims:
            sel_kwargs["ensemble_member"] = ds_raw.ensemble_member.values[0]
        test_val = ds_raw[variable].sel(sel_kwargs).isel(latitude=0, longitude=0).values
        if not np.isnan(float(test_val)):
            return t
    return pd.Timestamp(init_times[0])


def derive_dataset(
    *,
    store_config: dict[str, Any],
    output_path: Path,
    extent: dict[str, Any] | None,
    variable: str,
    period_type: str,
) -> None:
    """Derive a daily forecast zarr from a GEFS-like icechunk store.

    Steps:
    1. Select the most recent init_time with a complete forecast
    2. Average across ensemble members (ensemble mean)
    3. Map lead_time → valid_time (calendar dates) as the ``time`` dimension
    4. Subset to the instance extent when configured
    5. Resample 6-hourly steps to daily mean
    6. Trim trailing NaN time steps (unpublished lead_times still in transit)
    7. Write as a consolidated Zarr v3 store with ascending lat/lon order
    """
    import pandas as pd

    from climate_api.providers.remote_zarr import open_remote_dataset

    logger.info("Opening remote store for derived forecast '%s'", variable)
    ds_raw = open_remote_dataset(store_config)
    try:
        latest_init = most_recent_complete_init(ds_raw, variable=variable)
        logger.info("Selected init_time %s", latest_init.date())
        ds_run = ds_raw[[variable]].sel(init_time=latest_init)

        if "ensemble_member" in ds_run.dims:
            ds_run = ds_run.mean(dim="ensemble_member")

        valid_times = latest_init + pd.to_timedelta(ds_run.lead_time.values)
        ds_run = (
            ds_run.assign_coords(time=("lead_time", valid_times.values))
            .swap_dims({"lead_time": "time"})
            .drop_vars("lead_time", errors="ignore")
        )
        # Drop 2-D auxiliary coords (e.g. valid_time indexed by init_time×lead_time)
        # that become meaningless after selecting a single init_time.
        aux = [c for c in ds_run.coords if c not in ds_run.dims and c != variable]
        if aux:
            ds_run = ds_run.drop_vars(aux, errors="ignore")

        if extent is not None:
            xmin, ymin, xmax, ymax = extent["bbox"]
            lat = ds_run.latitude
            if float(lat.values[0]) > float(lat.values[-1]):
                ds_run = ds_run.sel(latitude=slice(ymax, ymin), longitude=slice(xmin, xmax))
            else:
                ds_run = ds_run.sel(latitude=slice(ymin, ymax), longitude=slice(xmin, xmax))

        ds_daily = ds_run.resample(time="1D").mean()

        # Drop trailing time steps that are entirely NaN — these are unpublished
        # lead_times that exist as placeholders in the icechunk store but have no
        # actual forecast data yet (the run is still being distributed).
        valid_mask = ds_daily[variable].isnull().all(dim=["latitude", "longitude"]).compute()
        first_all_nan = int(valid_mask.argmax().item()) if bool(valid_mask.any().item()) else len(valid_mask)  # type: ignore[union-attr]
        if first_all_nan < len(valid_mask):
            ds_daily = ds_daily.isel(time=slice(None, first_all_nan))

        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing derived forecast zarr to '%s'", output_path)
        # Ensure ascending lat/lon order (GEFS raw store is lat-descending).
        ds_daily = ds_daily.sortby(["latitude", "longitude"])
        # Resampling produces irregular dask chunks; rechunk to uniform before writing.
        ds_daily = ds_daily.chunk({"time": 10, "latitude": -1, "longitude": -1})
        ds_daily.to_zarr(output_path, mode="w", consolidated=True)
    finally:
        ds_raw.close()
