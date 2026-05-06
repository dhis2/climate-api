"""Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Climate API instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

from climate_api.client import Client

BASE_URL = "http://127.0.0.1:8000"


def main() -> None:
    """Discover and open the first published dataset."""
    api = Client(BASE_URL)
    datasets = api.catalog()

    if not datasets:
        print("No published datasets found. Run an ingestion first.")
        return

    print(datasets)

    first = datasets[0]
    print(f"\nOpening: {first['title']}")

    ds = api.open(first["id"])
    print(ds)

    print(f"\nTime range: {ds.time.values[0]}  →  {ds.time.values[-1]}")
    print(f"Time steps: {ds.sizes['time']}")
    print(f"Latitude:  {float(ds.latitude.min()):.4f}  →  {float(ds.latitude.max()):.4f}")
    print(f"Longitude: {float(ds.longitude.min()):.4f}  →  {float(ds.longitude.max()):.4f}")

    variable = list(ds.data_vars)[0]
    centre_lat = float((ds.latitude.min() + ds.latitude.max()) / 2)
    centre_lon = float((ds.longitude.min() + ds.longitude.max()) / 2)
    sample = ds[variable].isel(time=0).sel(latitude=centre_lat, longitude=centre_lon, method="nearest")
    print(f"\n{variable} at domain centre, t=0: {float(sample.values):.4f}")


if __name__ == "__main__":
    main()
