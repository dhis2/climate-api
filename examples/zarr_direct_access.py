"""Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Climate API instance with at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

from climate_api.client import Client

BASE_URL = "http://127.0.0.1:8000"


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

    print(f"\nDimensions:  {dict(ds.sizes)}")
    if "time" in ds.dims:
        print(f"Time range:  {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"y:    {ds.y.min().item():.4f}  →  {ds.y.max().item():.4f}")
    print(f"x:   {ds.x.min().item():.4f}  →  {ds.x.max().item():.4f}")

    variable = list(ds.data_vars)[0]

    # Select a single time step
    if "time" in ds.dims:
        t0 = ds.time.values[0]
        snap = ds[variable].sel(time=t0).compute()
        print(f"\n{variable} snapshot at {t0}:")
        print(f"  shape: {snap.shape},  min: {snap.min().item():.4f},  max: {snap.max().item():.4f}")

    # Select the point closest to the spatial centre of the domain
    cy, cx = ds.y.mean().item(), ds.x.mean().item()
    point = ds[variable].sel(y=cy, x=cx, method="nearest")
    print(f"\n{variable} at domain centre ({cy:.2f}, {cx:.2f}):")
    if "time" in ds.dims:
        print(point.to_dataframe()[[variable]].head(10))
    else:
        print(f"  value: {point.compute().item()}")

    # Spatial mean over the full domain — first 10 time steps
    if "time" in ds.dims:
        spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=["y", "x"])
        print(f"\nSpatial mean {variable} time series (first 10 steps):")
        print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
