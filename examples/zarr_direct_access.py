"""
Open a published GeoZarr dataset directly and demonstrate spatial and temporal subsetting.

Requires a running Climate API instance and a published CHIRPS3 dataset.
Adjust BASE_URL and DATASET_ID for your instance.
"""

import xarray as xr

BASE_URL = "http://127.0.0.1:8000"
DATASET_ID = "chirps3_precipitation_daily_sle"


def main() -> None:
    zarr_url = f"{BASE_URL}/zarr/{DATASET_ID}"
    print(f"Opening: {zarr_url}\n")

    ds = xr.open_zarr(zarr_url, consolidated=False)
    print(ds)

    # Dimensions and coordinates
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

    # Select a point (Freetown, Sierra Leone)
    freetown_lat, freetown_lon = 8.48, -13.23
    point = ds[variable].sel(latitude=freetown_lat, longitude=freetown_lon, method="nearest")
    print(f"\n{variable} at Freetown ({freetown_lat}N, {freetown_lon}E):")
    print(point.to_dataframe()[[variable]].head(10))

    # Spatial mean over the full domain — a simple time series
    spatial_mean = ds[variable].mean(dim=["latitude", "longitude"])
    print(f"\nSpatial mean {variable} time series:")
    print(spatial_mean.to_dataframe()[[variable]])


if __name__ == "__main__":
    main()
