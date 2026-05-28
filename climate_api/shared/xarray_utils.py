"""Utility helpers for xarray dimension name discovery."""

from __future__ import annotations

from typing import Any


def get_time_dim(ds: Any) -> str:
    """Return the name of the time dimension in a dataset or dataframe."""
    for name in ("t", "valid_time", "time"):
        if hasattr(ds, name):
            return name
    raise ValueError(f"Unable to find time dimension: {getattr(ds, 'coords', repr(ds))}")


def get_x_y_dims(ds: Any) -> tuple[str, str]:
    """Return ``(x_dim, y_dim)`` spatial dimension names from a dataset.

    Only considers actual dimensions (not 2-D auxiliary coordinates) so datasets
    with both x/y dimensions and lon/lat auxiliary coords are handled correctly.
    """
    actual_dims: set[str] = set(getattr(ds, "dims", {}) or {})
    for x_name, y_name in (("x", "y"), ("lon", "lat"), ("longitude", "latitude")):
        if x_name in actual_dims and y_name in actual_dims:
            return x_name, y_name
    raise ValueError(f"Unable to find spatial dimensions: {getattr(ds, 'coords', repr(ds))}")
