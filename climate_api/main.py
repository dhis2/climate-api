"""DHIS2 Climate API -- Climate and earth observation data API for DHIS2."""

import asyncio
import os
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

import climate_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from climate_api.data_registry import routes as dataset_template_routes
from climate_api.extents import routes as extent_routes
from climate_api.ingestions import routes as ingestion_routes
from climate_api.processing import routes as processing_routes
from climate_api.pygeoapi_app import mount_pygeoapi
from climate_api.stac import routes as stac_routes
from climate_api.system import routes as system_routes


def _zarr_browser_access_origins() -> set[str]:
    """Return allowlisted remote origins permitted to inspect local Zarr endpoints."""
    raw = os.getenv("CLIMATE_API_ZARR_BROWSER_ORIGINS", "https://inspect.geozarr.org")
    return {origin.strip() for origin in raw.split(",") if origin.strip()}


def _append_vary_value(response: Response, value: str) -> None:
    """Append one token to the Vary header without clobbering existing values."""
    existing = response.headers.get("Vary")
    if existing is None:
        response.headers["Vary"] = value
        return

    values = [item.strip() for item in existing.split(",") if item.strip()]
    if value not in values:
        response.headers["Vary"] = ", ".join([*values, value])


@asynccontextmanager
async def _lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Run background warmup tasks on startup so first requests hit warm caches."""
    asyncio.get_event_loop().run_in_executor(None, _warmup_remote_zarr_stores)
    yield


def _warmup_remote_zarr_stores() -> None:
    """Open and cache every published REMOTE_ZARR store at process startup."""
    from climate_api.ingestions.schemas import ArtifactFormat
    from climate_api.ingestions.services import _load_records
    from climate_api.providers.remote_zarr import warmup_remote_store
    from climate_api.data_registry.services import datasets as registry_datasets

    for record in _load_records():
        if record.format != ArtifactFormat.REMOTE_ZARR:
            continue
        source_dataset = registry_datasets.get_dataset(record.dataset_id) or {}
        store_config = source_dataset.get("store")
        if isinstance(store_config, dict):
            warmup_remote_store(store_config)


def create_app() -> FastAPI:
    """Create and configure the Climate API FastAPI application.

    This is the public entry point for embedding the API in a larger application:

        from climate_api.main import create_app
        app = create_app()
    """
    _app = FastAPI(lifespan=_lifespan)

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @_app.middleware("http")
    async def add_zarr_browser_access_headers(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Add browser access headers needed by remote Zarr inspectors calling localhost."""
        origin = request.headers.get("origin")
        allowed_origin = origin if origin in _zarr_browser_access_origins() else None
        if (
            request.method == "OPTIONS"
            and (request.url.path == "/zarr" or request.url.path.startswith("/zarr/"))
            and request.headers.get("access-control-request-private-network") == "true"
            and allowed_origin is not None
        ):
            response = Response(status_code=200)
        else:
            response = await call_next(request)
        if request.url.path == "/zarr" or request.url.path.startswith("/zarr/"):
            if allowed_origin is not None:
                response.headers["Access-Control-Allow-Origin"] = allowed_origin
                _append_vary_value(response, "Origin")
                response.headers.setdefault("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
                response.headers.setdefault(
                    "Access-Control-Allow-Headers",
                    request.headers.get("access-control-request-headers", "*"),
                )
            if allowed_origin is not None and request.headers.get("access-control-request-private-network") == "true":
                response.headers["Access-Control-Allow-Private-Network"] = "true"
        return response

    _app.include_router(system_routes.router, tags=["System"])
    _app.include_router(stac_routes.router, prefix="/stac", tags=["STAC"])
    _app.include_router(extent_routes.router, prefix="/extent", tags=["Extent"])
    _app.include_router(dataset_template_routes.router, prefix="/dataset-templates", tags=["Dataset templates"])
    _app.include_router(ingestion_routes.datasets_router, prefix="/datasets", tags=["Datasets"])
    _app.include_router(ingestion_routes.ingestions_router, prefix="/ingestions", tags=["Ingestions"])
    _app.include_router(ingestion_routes.zarr_router, prefix="/zarr", tags=["Zarr"])
    _app.include_router(ingestion_routes.sync_router, prefix="/sync", tags=["Sync"])
    _app.include_router(processing_routes.router, prefix="/processes", tags=["Processes"])

    mount_pygeoapi(_app)

    return _app


app = create_app()
