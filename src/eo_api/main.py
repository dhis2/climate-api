"""DHIS2 EO API -- Earth observation data API for DHIS2."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import eo_api.startup  # noqa: F401  # pyright: ignore[reportUnusedImport]
from eo_api import data_accessor, data_manager, data_registry, system
from eo_api.ogc_api import ogc_api_app
from eo_api.tiles import tiles_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(system.routes.router, tags=['System'])
app.include_router(data_registry.routes.router, prefix='/registry', tags=['Data registry'])
app.include_router(data_manager.routes.router, prefix='/manage', tags=['Data manager'])
app.include_router(data_accessor.routes.router, prefix='/retrieve', tags=['Data retrieval'])
app.include_router(tiles_router, prefix='/zarr', tags=['Zarr'])

app.mount("/data", StaticFiles(directory="data/downloads"), name="Data")

# mount all pygeoapi endpoints to /ogcapi
app.mount(path="/ogcapi", app=ogc_api_app)
