"""DHIS2 EO API -- Earth observation data API for DHIS2."""

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

@app.middleware("http")
async def add_zarr_browser_access_headers(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Add browser access headers needed by remote Zarr inspectors calling localhost."""
    if (
        request.method == "OPTIONS"
        and (request.url.path == "/zarr" or request.url.path.startswith("/zarr/"))
        and request.headers.get("access-control-request-private-network") == "true"
    ):
        response = Response(status_code=200)
    else:
        response = await call_next(request)
    if request.url.path == "/zarr" or request.url.path.startswith("/zarr/"):
        origin = request.headers.get("origin")
        if origin is not None:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Vary"] = "Origin"
            response.headers.setdefault("Access-Control-Allow-Methods", "GET, OPTIONS")
            response.headers.setdefault(
                "Access-Control-Allow-Headers",
                request.headers.get("access-control-request-headers", "*"),
            )
        if request.headers.get("access-control-request-private-network") == "true":
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
