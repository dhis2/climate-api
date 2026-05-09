"""Deaccumulation transforms for ERA5 accumulated fields."""

from typing import Any

import xarray as xr


def deaccumulate_era5(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    """Convert ERA5 accumulated fields to per-step values by forward differencing.

    ERA5 stores precipitation and other flux variables as accumulations from the
    start of the forecast step. This subtracts consecutive steps so each value
    represents the amount in that step alone, then clips negative artefacts.
    """
    varname = dataset["variable"]
    da = ds[varname]
    time_dim = next(d for d in da.dims if "time" in d)
    diff = da.diff(dim=time_dim)
    diff = diff.clip(min=0)
    # Drop the first time step (no previous step to diff against) and reassign.
    ds = ds.sel({time_dim: ds[time_dim][1:]})
    return ds.assign({varname: diff.assign_attrs(da.attrs)})
