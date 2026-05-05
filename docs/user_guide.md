# User Guide

This guide shows how to discover and access data from a running Climate API instance. It assumes the API is running locally at `http://127.0.0.1:8000` and that at least one dataset has been ingested and published.

For configuring a new instance for your country, see [setup_guide.md](setup_guide.md). For the full ingestion and sync API reference, see [managed_data_api_guide.md](managed_data_api_guide.md).

## Discovering datasets

The STAC catalog is the starting point for data discovery. It lists all published GeoZarr datasets as STAC Collections.

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq
```

Each entry in `links` with `"rel": "child"` points to one dataset collection:

```json
{
  "rel": "child",
  "href": "http://127.0.0.1:8000/stac/collections/chirps3_precipitation_daily_sle",
  "title": "Total precipitation (CHIRPS3)",
  "type": "application/json"
}
```

Fetch a collection to see its full metadata, including spatial and temporal extent and the Zarr asset:

```bash
curl -s http://127.0.0.1:8000/stac/collections/chirps3_precipitation_daily_sle | jq
```

The `assets.zarr` field contains everything needed to open the dataset:

```json
{
  "assets": {
    "zarr": {
      "href": "http://127.0.0.1:8000/zarr/chirps3_precipitation_daily_sle",
      "xarray:open_kwargs": { "consolidated": false }
    }
  }
}
```

## Opening a dataset with xarray

Use the STAC collection to open a dataset directly with xarray — no knowledge of internal paths or formats required:

```python
import requests
import xarray as xr

collection = requests.get(
    "http://127.0.0.1:8000/stac/collections/chirps3_precipitation_daily_sle"
).json()

asset = collection["assets"]["zarr"]
ds = xr.open_zarr(
    asset["href"],
    consolidated=asset["xarray:open_kwargs"]["consolidated"],
)
print(ds)
```

Each dataset has a time dimension, two spatial dimensions, and one data variable matching the climate variable (e.g. `precip` for CHIRPS, `t2m` for ERA5-Land temperature). Coordinate names vary by source:

| Coordinate | CHIRPS / WorldPop | ERA5-Land |
| ---------- | ----------------- | --------- |
| Time       | `time`            | `valid_time` |
| Latitude   | `y`               | `lat`     |
| Longitude  | `x`               | `lon`     |

Detect the actual names before selecting:

```python
time_dim = "valid_time" if "valid_time" in ds.coords else "time"
coords = set(ds.coords)
if "lat" in coords:
    y_dim, x_dim = "lat", "lon"
elif "latitude" in coords:
    y_dim, x_dim = "latitude", "longitude"
else:
    y_dim, x_dim = "y", "x"
```

Select a time slice:

```python
daily = ds.sel({time_dim: "2024-01-15"})
print(daily)
```

Select a spatial point closest to a location (e.g. Freetown, Sierra Leone):

```python
point = ds.sel({y_dim: 8.48, x_dim: -13.23}, method="nearest")
print(point["precip"].values)
```

Compute the spatial mean over the full domain for each time step:

```python
spatial_mean = ds["precip"].mean(dim=[y_dim, x_dim])
print(spatial_mean.to_dataframe())
```

## Dataset IDs

Dataset IDs follow the pattern `{source_dataset_id}_{extent_id}`, for example `chirps3_precipitation_daily_sle` for CHIRPS3 daily precipitation over Sierra Leone (`sle`).

Available datasets in the built-in catalogue:

| Dataset ID prefix              | Variable | Period | Source          |
| ------------------------------ | -------- | ------ | --------------- |
| `chirps3_precipitation_daily`  | `precip` | Daily  | CHIRPS v3       |
| `era5land_temperature_hourly`  | `t2m`    | Hourly | ERA5-Land       |
| `era5land_precipitation_hourly`| `tp`     | Hourly | ERA5-Land       |
| `worldpop_population_yearly`   | `pop`    | Yearly | WorldPop        |

## What's next

- See [`examples/stac_discover_and_open.py`](../examples/stac_discover_and_open.py) for a complete discovery and access script.
- See [`examples/zarr_direct_access.py`](../examples/zarr_direct_access.py) for direct Zarr access and spatial/temporal subsetting.
- See [managed_data_api_guide.md](managed_data_api_guide.md) for the full ingestion and sync API reference.
