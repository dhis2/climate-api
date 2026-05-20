"""Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Climate API instance with at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

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
    """Open a Zarr store directly and demonstrate spatial and temporal subsetting."""
    api = Client(BASE_URL)
    datasets = api.catalog()
    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    first = datasets[0]
    print(f"Opening: {first['title']}\n")

    ds = api.open(first["id"])
    print(ds)

    y_dim, x_dim = _spatial_dims(ds)

    print(f"\nDimensions:  {dict(ds.sizes)}")
    if "time" in ds.dims:
        print(f"Time range:  {ds.time.values[0]}  →  {ds.time.values[-1]}")
    if y_dim:
        print(f"{y_dim}:    {ds[y_dim].min().item():.4f}  →  {ds[y_dim].max().item():.4f}")
    if x_dim:
        print(f"{x_dim}:   {ds[x_dim].min().item():.4f}  →  {ds[x_dim].max().item():.4f}")

    variable = list(ds.data_vars)[0]

    # Select a single time step
    if "time" in ds.dims:
        t0 = ds.time.values[0]
        snapshot = ds[variable].sel(time=t0)
        print(f"\n{variable} snapshot at {t0}:")
        snap = snapshot.compute()
        print(f"  shape: {snap.shape},  min: {snap.min().item():.4f},  max: {snap.max().item():.4f}")

    # Select the point closest to the spatial centre of the domain
    if y_dim and x_dim:
        centre = {
            y_dim: ds[y_dim].mean().item(),
            x_dim: ds[x_dim].mean().item(),
        }
        point = ds[variable].sel(**centre, method="nearest")
        print(f"\n{variable} at domain centre ({centre[y_dim]:.2f}, {centre[x_dim]:.2f}):")
        if "time" in ds.dims:
            print(point.to_dataframe()[[variable]].head(10))
        else:
            print(f"  value: {point.compute().item()}")

    # Spatial mean over the full domain — first 10 time steps
    if "time" in ds.dims and y_dim and x_dim:
        spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=[y_dim, x_dim])
        print(f"\nSpatial mean {variable} time series (first 10 steps):")
        print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
