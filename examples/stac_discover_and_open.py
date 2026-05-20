"""Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Climate API instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import json

import xarray as xr

from climate_api.client import Client

BASE_URL = "http://127.0.0.1:8000"


def _spatial_dims(ds: xr.Dataset) -> tuple[str | None, str | None]:
    """Return (y_dim, x_dim) from the dataset's dimension names."""
    dims = [str(d) for d in ds.dims]
    y_dim = next((d for d in dims if d.lower() in ("y", "latitude", "lat")), None)
    x_dim = next((d for d in dims if d.lower() in ("x", "longitude", "lon")), None)
    return y_dim, x_dim


def main() -> None:
    """Discover and open the first published dataset."""
    api = Client(BASE_URL)
    datasets = api.catalog()

    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    print(json.dumps(datasets, indent=2))

    first = datasets[0]
    print(f"\nOpening: {first['title']}")

    ds = api.open(first["id"])
    print(ds)

    y_dim, x_dim = _spatial_dims(ds)

    if "time" in ds.dims:
        print(f"\nTime range: {ds.time.values[0]}  →  {ds.time.values[-1]}")
        print(f"Time steps: {ds.sizes['time']}")
    if y_dim:
        print(f"{y_dim}:  {ds[y_dim].min().item():.4f}  →  {ds[y_dim].max().item():.4f}")
    if x_dim:
        print(f"{x_dim}: {ds[x_dim].min().item():.4f}  →  {ds[x_dim].max().item():.4f}")

    variable = list(ds.data_vars)[0]
    if y_dim and x_dim:
        centre = {
            y_dim: ds[y_dim].mean().item(),
            x_dim: ds[x_dim].mean().item(),
        }
        selector = {"time": 0} if "time" in ds.dims else {}
        sample = ds[variable].isel(**selector).sel(**centre, method="nearest").compute()
        print(f"\n{variable} at domain centre, {'t=0' if selector else 'static'}: {sample.item()}")


if __name__ == "__main__":
    main()
