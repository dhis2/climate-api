"""TiTiler router definitions."""

from titiler.xarray.factory import TilerFactory  # pyright: ignore[reportMissingImports]
from titiler.xarray.io import Reader  # pyright: ignore[reportMissingImports]
from titiler.xarray.extensions import VariablesExtension  # pyright: ignore[reportMissingImports]

# Xarray-backed TiTiler endpoints (e.g. Zarr datasets).
tiles_router = TilerFactory(
    reader=Reader,
    extensions=[VariablesExtension()]
).router
