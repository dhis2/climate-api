"""DHIS2 EO API -- Earth observation data API for DHIS2."""

import os
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api.data_registry import routes as dataset_template_routes
from eo_api.extents import routes as extent_routes
from eo_api.ingestions import routes as ingestion_routes
from eo_api.pygeoapi_app import mount_pygeoapi
from eo_api.system import routes as system_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _zarr_browser_access_origins() -> set[str]:
    """Return allowlisted remote origins permitted to inspect local Zarr endpoints."""
    raw = os.getenv("EO_API_ZARR_BROWSER_ORIGINS", "https://inspect.geozarr.org")
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


@app.middleware("http")
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


app.include_router(system_routes.router, tags=["System"])
app.include_router(extent_routes.router, prefix="/extents", tags=["Extents"])
app.include_router(dataset_template_routes.router, prefix="/dataset-templates", tags=["Dataset templates"])
app.include_router(ingestion_routes.datasets_router, prefix="/datasets", tags=["Datasets"])
app.include_router(ingestion_routes.ingestions_router, prefix="/ingestions", tags=["Ingestions"])
app.include_router(ingestion_routes.zarr_router, prefix="/zarr", tags=["Zarr"])
app.include_router(ingestion_routes.sync_router, prefix="/sync", tags=["Sync"])

mount_pygeoapi(app)
