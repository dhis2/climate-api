"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api.data_registry import routes as dataset_template_routes
from eo_api.extents import routes as extent_routes
from eo_api.ingestions import routes as ingestion_routes
from eo_api.pygeoapi_app import mount_pygeoapi
from eo_api.system import routes as system_routes
from eo_api.tiles import titiler_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system_routes.router, tags=["System"])
app.include_router(extent_routes.router, prefix="/extents", tags=["Extents"])
app.include_router(dataset_template_routes.router, prefix="/dataset-templates", tags=["Dataset templates"])
app.include_router(ingestion_routes.datasets_router, prefix="/datasets", tags=["Datasets"])
app.include_router(ingestion_routes.ingestions_router, prefix="/ingestions", tags=["Ingestions"])
app.include_router(ingestion_routes.zarr_router, prefix="/zarr", tags=["Zarr"])
app.include_router(ingestion_routes.sync_router, prefix="/sync", tags=["Sync"])
app.include_router(titiler_routes.router, prefix='/titiler', tags=["TiTiler"])

mount_pygeoapi(app)
