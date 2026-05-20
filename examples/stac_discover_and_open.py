"""Discover published datasets via the STAC catalog and open one with xarray.

Requires a running Climate API instance and at least one published dataset.
Adjust BASE_URL if the API is not running on the default local address.
"""

import json

from climate_api.client import Client

BASE_URL = "http://127.0.0.1:8000"


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

    if "time" in ds.dims:
        print(f"\nTime range: {ds.time.values[0]}  →  {ds.time.values[-1]}")
        print(f"Time steps: {ds.sizes['time']}")
    print(f"y:  {ds.y.min().item():.4f}  →  {ds.y.max().item():.4f}")
    print(f"x: {ds.x.min().item():.4f}  →  {ds.x.max().item():.4f}")

    variable = list(ds.data_vars)[0]
    centre = {"y": ds.y.mean().item(), "x": ds.x.mean().item()}
    selector = {"time": 0} if "time" in ds.dims else {}
    sample = ds[variable].isel(**selector).sel(**centre, method="nearest").compute()
    print(f"\n{variable} at domain centre, {'t=0' if selector else 'static'}: {sample.item()}")


if __name__ == "__main__":
    main()
