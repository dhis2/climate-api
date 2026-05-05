"""Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Climate API instance and a published CHIRPS3 dataset.
Adjust BASE_URL and DATASET_ID for your instance.
"""

import requests
import xarray as xr

BASE_URL = "http://127.0.0.1:8000"
DATASET_ID = "chirps3_precipitation_daily_sle"


def main() -> None:
    """Open a Zarr store directly and demonstrate spatial and temporal subsetting."""
    # Fetch open kwargs from the STAC collection to get the correct consolidated flag
    collection = requests.get(f"{BASE_URL}/stac/collections/{DATASET_ID}").json()
    asset = collection["assets"]["zarr"]
    zarr_url = asset["href"]
    open_kwargs = asset["xarray:open_kwargs"]

    print(f"Opening: {zarr_url}\n")
    ds = xr.open_zarr(zarr_url, **open_kwargs)
    print(ds)

    # Coordinate names vary by dataset: ERA5-Land uses valid_time and lon/lat;
    # CHIRPS and WorldPop use time and x/y (or longitude/latitude)
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

    # Select a point (Freetown, Sierra Leone)
    freetown_lat, freetown_lon = 8.48, -13.23
    point = ds[variable].sel({y_dim: freetown_lat, x_dim: freetown_lon}, method="nearest")
    lat_label = f"{abs(freetown_lat)}{'N' if freetown_lat >= 0 else 'S'}"
    lon_label = f"{abs(freetown_lon)}{'E' if freetown_lon >= 0 else 'W'}"
    print(f"\n{variable} at Freetown ({lat_label}, {lon_label}):")
    print(point.to_dataframe()[[variable]].head(10))

    # Spatial mean over the full domain — a simple time series
    spatial_mean = ds[variable].mean(dim=[y_dim, x_dim])
    print(f"\nSpatial mean {variable} time series:")
    print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
