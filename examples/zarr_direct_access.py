"""Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Climate API instance with at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import requests
import xarray as xr

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    """Open a Zarr store directly and demonstrate spatial and temporal subsetting."""
    # Discover the first published dataset from the catalog
    catalog_response = requests.get(f"{BASE_URL}/stac/catalog.json")
    catalog_response.raise_for_status()
    catalog = catalog_response.json()
    collection_url = next((link["href"] for link in catalog["links"] if link["rel"] == "child"), None)
    if collection_url is None:
        print("No published datasets found. Run an ingestion first.")
        return

    collection_response = requests.get(collection_url)
    collection_response.raise_for_status()
    collection = collection_response.json()
    asset = collection["assets"]["zarr"]
    zarr_url = asset["href"]
    open_kwargs = asset["xarray:open_kwargs"]

    print(f"Opening: {zarr_url}\n")
    ds = xr.open_zarr(zarr_url, **open_kwargs)
    print(ds)

    # Coordinate names are preserved from the source and vary by dataset.
    time_dim = "valid_time" if "valid_time" in ds.coords else "time"
    coords = set(ds.coords)
    if "lat" in coords:
        y_dim, x_dim = "lat", "lon"
    elif "latitude" in coords:
        y_dim, x_dim = "latitude", "longitude"
    else:
        y_dim, x_dim = "y", "x"

    # Dimensions and coordinates
    print(f"\nDimensions:  {dict(ds.sizes)}")
    print(f"Time range:  {ds[time_dim].values[0]}  →  {ds[time_dim].values[-1]}")
    print(f"Latitude:    {float(ds[y_dim].min()):.4f}  →  {float(ds[y_dim].max()):.4f}")
    print(f"Longitude:   {float(ds[x_dim].min()):.4f}  →  {float(ds[x_dim].max()):.4f}")

    variable = list(ds.data_vars)[0]

    # Select a single time step
    t0 = ds[time_dim].values[0]
    snapshot = ds[variable].sel({time_dim: t0})
    print(f"\n{variable} snapshot at {t0}:")
    print(f"  shape: {snapshot.shape},  min: {float(snapshot.min()):.4f},  max: {float(snapshot.max()):.4f}")

    # Select the point closest to the spatial centre of the domain
    centre_y = float((ds[y_dim].min() + ds[y_dim].max()) / 2)
    centre_x = float((ds[x_dim].min() + ds[x_dim].max()) / 2)
    point = ds[variable].sel({y_dim: centre_y, x_dim: centre_x}, method="nearest")
    print(f"\n{variable} at domain centre ({centre_y:.2f}, {centre_x:.2f}):")
    print(point.to_dataframe()[[variable]].head(10))

    # Spatial mean over the full domain — a simple time series
    spatial_mean = ds[variable].mean(dim=[y_dim, x_dim])
    print(f"\nSpatial mean {variable} time series:")
    print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
