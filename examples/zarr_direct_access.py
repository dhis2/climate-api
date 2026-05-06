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
    dataset_id = first["href"].rstrip("/").split("/")[-1]
    print(f"Opening: {first['title']}\n")

    ds = api.open(dataset_id)
    print(ds)

    print(f"\nDimensions:  {dict(ds.sizes)}")
    print(f"Time range:  {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"Latitude:    {float(ds.latitude.min()):.4f}  →  {float(ds.latitude.max()):.4f}")
    print(f"Longitude:   {float(ds.longitude.min()):.4f}  →  {float(ds.longitude.max()):.4f}")

    variable = list(ds.data_vars)[0]

    # Select a single time step
    t0 = ds.time.values[0]
    snapshot = ds[variable].sel(time=t0)
    print(f"\n{variable} snapshot at {t0}:")
    print(f"  shape: {snapshot.shape},  min: {float(snapshot.min()):.4f},  max: {float(snapshot.max()):.4f}")

    # Select the point closest to the spatial centre of the domain
    centre_lat = float((ds.latitude.min() + ds.latitude.max()) / 2)
    centre_lon = float((ds.longitude.min() + ds.longitude.max()) / 2)
    point = ds[variable].sel(latitude=centre_lat, longitude=centre_lon, method="nearest")
    print(f"\n{variable} at domain centre ({centre_lat:.2f}, {centre_lon:.2f}):")
    print(point.to_dataframe()[[variable]].head(10))

    # Spatial mean over the full domain — first 10 time steps
    spatial_mean = ds[variable].isel(time=slice(10)).mean(dim=["latitude", "longitude"])
    print(f"\nSpatial mean {variable} time series (first 10 steps):")
    print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
