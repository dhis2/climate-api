# CLAUDE.md

## Project overview

The DHIS2 Climate API is a FastAPI-based REST API that downloads, processes, and serves Climate and Earth Observation data.

Key concepts:

- **Dataset templates** — YAML files in `data/datasets/` describing a data source (variable, period type, download function). These are blueprints.
- **Artifacts / managed datasets** — ingested instances of a template for a specific spatial extent and time range. Exposed under `/datasets` and `/zarr/{dataset_id}`.
- **Extents** — named spatial bounding boxes configured at instance setup time (`id`, `bbox`, optional `country_code`).
- **GeoZarr stores** — all output is written as zarr v3 with GeoZarr metadata. Datasets with `multiscales` in their cache config are written as multiscale pyramids; others are flat zarr stores.
- **Pyramid vs flat zarr** — pyramid stores (e.g.WorldPop) are served via `/zarr/{dataset_id}` and detected by the presence of a `0/` subdirectory. Flat stores (e.g. CHIRPS) are also served via `/zarr` and additionally exposed through pygeoapi under `/ogcapi`.

## Repository layout

```
src/eo_api/
  data_manager/     # download and zarr build (downloader.py)
  data_accessor/    # open zarr / netcdf for read (accessor.py)
  data_registry/    # dataset template YAML loading
  ingestions/       # artifact lifecycle: create, list, sync, publish
  publications/     # pygeoapi config generation
  extents/          # spatial extent config
  shared/           # dhis2 adapter, time utils
  main.py           # FastAPI app, CORS middleware, route registration
data/datasets/      # dataset template YAMLs (chirps3.yaml, worldpop.yaml, …)
config/pygeoapi/    # pygeoapi base config
tests/
docs/
```

## Development

```bash
make run      # start uvicorn with --reload (also generates pygeoapi OpenAPI spec)
make lint     # ruff check + ruff format + mypy + pyright
make test     # pytest
make start    # docker compose up --build
```

The `.env` file is required for `make run` and `make openapi`. Copy `.env.example` if it exists.

## Dataset templates

Each YAML in `data/datasets/` defines a dataset template. The `cache_info` block controls download and zarr build behaviour:

```yaml
cache_info:
  eo_function: dhis2eo.data.worldpop.pop_total.yearly.download
  default_params: {} # passed to the download function
  multiscales: # optional — triggers pyramid build
    levels: 4
    method: mean
```

If `multiscales` is present, `build_dataset_zarr` builds a topozarr pyramid. Otherwise it writes a flat chunked zarr with auto-computed chunk sizes tuned to the dataset's `period_type`.

## pygeoapi

pygeoapi is mounted at `/ogcapi` as a sub-application. Its config is generated dynamically from published artifacts by `publications/services.py` and written to `data/pygeoapi/pygeoapi-config.yml`.

The config is regenerated on each `publish_artifact` call and also at startup via `ensure_pygeoapi_base_config()`.

## CORS / browser access

The `/zarr` routes require special CORS handling for Private Network Access preflights (zarr-layer calling localhost from a remote origin). This is implemented as a custom middleware in `main.py` that intercepts OPTIONS requests and adds `Access-Control-Allow-Private-Network: true` and `Access-Control-Allow-Methods: GET, HEAD, OPTIONS`.

## Commit conventions

- Use conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`)
- No Co-Authored-By or other attribution lines
- Never use emojis anywhere — not in commits, code, comments, or responses
