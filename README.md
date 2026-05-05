# DHIS2 Climate API

Climate and Earth Observation data is distributed across dozens of providers — each with different APIs, data formats, and access mechanisms. The Climate API unifies this fragmented landscape behind a single, consistent interface.

Each instance is configured for a specific country or region, and all data extraction, processing, and storage is scoped to that spatial extent. It abstracts data access across heterogeneous sources (CHIRPS, ERA5, WorldPop, and others), stores outputs as GeoZarr, and exposes them through standards-based endpoints.

The platform is designed to operate independently of DHIS2 and can be deployed on local, cloud-hosted, or sovereign country infrastructure. See [docs/project_description.md](docs/project_description.md) for a full description of the vision and technical architecture, [docs/managed_data_api_guide.md](docs/managed_data_api_guide.md) for the current API surface, and [docs/roadmap.md](docs/roadmap.md) for the planned development steps.

> **Status: active development.** Current focus is on dataset ingestion, sync workflows, and GeoZarr storage. APIs and data models may change without notice.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

```
uv sync
```

Copy `.env.example` to `.env` and adjust values as needed. Environment variables are loaded automatically from `.env` at runtime.

Key environment variables:

- `DHIS2_BASE_URL` — DHIS2 API base URL (defaults to play server in `.env.example`)
- `DHIS2_USERNAME` — DHIS2 username
- `DHIS2_PASSWORD` — DHIS2 password

Start the app:

```
uv run uvicorn climate_api.main:app --reload
```

### Using pip

If you cannot use uv (e.g. mixed conda/forge environments):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn climate_api.main:app --reload
```

### Using conda

```
conda create -n dhis2-climate-api python=3.13
conda activate dhis2-climate-api
pip install -e .
uvicorn climate_api.main:app --reload
```

## Development

Common Makefile targets:

| Target         | Description                                      |
| -------------- | ------------------------------------------------ |
| `make sync`    | Install dependencies with uv                     |
| `make run`     | Start the app with uvicorn (hot reload)          |
| `make lint`    | Check linting, formatting, and types             |
| `make fix`     | Autofix ruff lint and format issues              |
| `make test`    | Run the test suite with pytest                   |
| `make openapi` | Regenerate the pygeoapi OpenAPI spec             |
| `make start`   | Build and start the Docker stack                 |
| `make restart` | Tear down, rebuild, and restart the Docker stack |

## Endpoints

Once running, the API is available at:

| Endpoint                                  | Description                                |
| ----------------------------------------- | ------------------------------------------ |
| `http://localhost:8000/`                  | Welcome / health check                     |
| `http://localhost:8000/docs`              | Interactive API documentation (Swagger UI) |
| `http://localhost:8000/stac/catalog.json` | STAC catalog for published GeoZarr data    |
| `http://localhost:8000/ogcapi`            | OGC API root                               |
| `http://localhost:8000/zarr/{dataset_id}` | GeoZarr store for a published dataset      |

## STAC

Published Zarr-backed managed datasets are exposed under `/stac` as one STAC Collection per dataset.

- `/stac/catalog.json` is the entrypoint catalog
- `/stac/collections/{dataset_id}` exposes a Collection with a direct `/zarr/{dataset_id}` asset href
- `xstac` derives Datacube metadata from the real Zarr-backed dataset
- `pygeoapi` remains the OGC query layer under `/ogcapi`

Minimal example:

```python
import requests
import xarray as xr

collection = requests.get(
    "http://127.0.0.1:8000/stac/collections/chirps3_precipitation_daily_sle"
).json()

asset = collection["assets"]["zarr"]
ds = xr.open_zarr(asset["href"], consolidated=asset["xarray:open_kwargs"]["consolidated"])
```

## pygeoapi

The OGC API is served by pygeoapi, mounted at `/ogcapi`. Its configuration is generated dynamically from published artifacts and written to `data/pygeoapi/pygeoapi-config.yml`.

To validate the configuration manually:

```
PYTHONPATH="$(pwd)/src" uv run pygeoapi config validate -c data/pygeoapi/pygeoapi-config.yml
```

Regenerate after changes to `config/pygeoapi/base.yml` or publication logic:

```
make openapi
```
