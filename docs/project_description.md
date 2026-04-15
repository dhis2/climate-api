# DHIS2 Climate API

## Climate Data & Earth Observation Integration Platform

_Project Description · 2026_

---

## 1. Background

Climate change is significantly increasing the burden of vector-borne and weather-sensitive diseases in low- and middle-income countries, yet national health systems lack the tools to systematically integrate climate data and earth observation into disease surveillance and early warning. Climate and earth observation data is distributed across dozens of providers — each with different APIs, data formats and access mechanisms. This makes systematic integration very complex for national health programmes with limited technical capacity.

This project develops the DHIS2 Climate API — an open-source, standards-based and decentralized integration platform that unifies this fragmented space behind a single, consistent interface. By abstracting data access across heterogeneous sources and harmonising outputs into a common format, the platform enables national health programmes to automatically ingest, process, and analyse global, national, and local climate and environmental data without specialised expertise for each data provider. Although developed in close alignment with DHIS2, the platform is designed to operate independently of any specific health information system, and can serve as shared infrastructure for any application that requires systematic access to harmonised climate and environmental data.

The data is stored and served using open geospatial standards — ensuring that the platform is not a closed system but an open foundation. By adhering to these standards, third-party services, national meteorological offices, and local developers can connect their own data sources, build custom analytical workflows, and extend the platform to address locally specific needs. This openness is a deliberate design choice: the goal is not a single monolithic tool, but shared infrastructure that countries and communities can adapt and innovate upon.

A key design principle is data sovereignty — the platform is deployable on national or regional infrastructure without dependency on proprietary services, ensuring that countries retain full control over their data. The DHIS2 Climate API will be developed in close collaboration with HISP groups in the countries themselves — the people who understand local data landscapes, institutional arrangements, and technical constraints — ensuring that the platform addresses real needs. By bridging the gap between earth observation science and routine public health decision-making, this project will strengthen climate-resilient health systems.

---

## 2. Overview

The DHIS2 Climate API is a no-code data integration platform that enables earth observation (EO) and climate data from multiple upstream sources to be downloaded, processed, harmonised, and loaded into DHIS2 and the CHAP Modelling Platform. It is designed to supplement and eventually replace the current reliance on Google Earth Engine in DHIS2.

The platform is built as a Python-based REST API (FastAPI) and exposes both native endpoints and OGC API-compliant endpoints (via pygeoapi). Data is stored in cloud-native GeoZarr format and can be consumed by the DHIS2 Climate App, the DHIS2 Maps App, DHIS2 Climate Tools, the CHAP Modelling Platform, and third-party tools such as QGIS.

The Climate API is envisioned as the shared data infrastructure layer for the DHIS2 climate and health ecosystem — a single, well-defined source of spatiotemporal raster data that any DHIS2 application or external tool can build on.

### 2.1 Relationship to existing DHIS2 climate work

The Climate API supplements and extends the existing DHIS2 climate data integration work documented at dhis2.org/climate/climate-data/. It is built on the same underlying Python libraries — dhis2eo and dhis2-python-client — but wraps them in a standardised API so that data workflows that currently require Python scripting or Google Earth Engine access can be configured and run without writing code.

### 2.2 Scope of this document

This document describes the project vision, design constraints, user stories, functional requirements, technical architecture, and data pipeline approach. It is intended for technical contributors, DHIS2 country implementers, and stakeholders evaluating the Climate API for deployment.

---

## 3. Vision and goals

The Climate API aims to:

- Provide a unified API through which EO and climate data can be requested, downloaded, processed, and uploaded to DHIS2 — with all complexity handled behind the scenes.
- Replace the current usage of Google Earth Engine for on-the-fly image tiling, point queries, and org unit aggregation.
- Serve as a no-code alternative to DHIS2 Climate Tools for standard data integration workflows, built on the same underlying libraries.
- Allow DHIS2 Climate/Maps app and CHAP to act as frontends consuming the Climate API.
- Support custom orchestration — users can build pipelines with pre- and post-processing steps.
- Work independently of a DHIS2 instance.
- Follow the requirements for being a Digital Public Good (DPG) and adhere to the FAIR principles (Findable, Accessible, Interoperable, Reusable).

---

## 4. User stories

| ID   | Actor        | Goal                                                                                                                                                   |
| ---- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| US-A | Data manager | Import daily temperature and precipitation data into DHIS2 at a user-defined scheduled interval (e.g. nightly), automatically aggregated to org units. |
| US-B | Data manager | Import population data for the current year, automatically aggregated to org units.                                                                    |
| US-C | Analyst      | Visualise high-resolution population data on DHIS2 Maps with styles adapted to the population density of the country.                                  |
| US-D | Analyst      | Preview climate data for an org unit of interest before importing it to DHIS2.                                                                         |
| US-E | Data manager | Add a custom pre- or post-processing step (e.g. calculate consecutive rainy days) before importing the result to DHIS2.                                |

---

## 5. Design constraints

The following constraints apply to the first version of the Climate API. They represent deliberate architectural decisions and are open for discussion as the platform matures.

### 5.1 Single spatial extent

Each Climate API instance is configured with one or more named extents, defined at setup time. Each extent has a required `id` and `bbox`, and an optional `org_unit_id` for linking to a DHIS2 org unit. For example:

```yaml
extents:
  - id: sle
    bbox: [-13.5, 6.9, -10.1, 10.0]
    org_unit_id: ... # optional
```

Extents are not expected to change after setup. For the first version, only a single extent is supported. Larger countries may configure a sub-national extent (e.g. a district) to limit initial download volume. The `extent_id` is passed as a parameter to ingestion alongside the `dataset_id` — ingestion is not tied to a DHIS2 instance.

### 5.2 No temporal gaps

Downloaded datasets must not contain temporal gaps, unless gaps exist in the original upstream data source. All subsequent scheduled updates look at the last period with data and import from there until today, ensuring continuity. The `/sync` endpoint validates temporal continuity before appending new time steps.

### 5.3 One period type per dataset

Each dataset has a single period type (daily, weekly, monthly, yearly, etc.). The period type is included in the dataset ID (e.g. `chirps3_precipitation_daily_sle`). It is possible to construct derived datasets with a different period type from an existing dataset (e.g. daily → weekly aggregation), which will result in a separate dataset ID.

### 5.4 One artifact per dataset

Each dataset ID maps to exactly one output artifact in the form of a GeoZarr store. New time steps are appended to the existing store on sync rather than creating parallel stores.

### 5.5 DHIS2-independent operation

Core parts of the Climate API must function without a connected DHIS2 instance. Spatial extent is defined via instance configuration rather than a DHIS2 org unit query. Aggregation accepts GeoJSON features from any source and outputs CSV or JSON as well as DHIS2 data values.

### 5.6 Dataset templates and published datasets

Internally, the Climate API distinguishes between dataset templates and published datasets:

- **Dataset templates** — YAML definitions describing a dataset type (source, variable, period type, processing steps). These are internal and align closely with the OGC API Collections specification. They act as blueprints for ingestion.
- **Published datasets** — actualised, ingested datasets for a specific extent and time range, exposed under `/datasets` and `/ogcapi/collections`. These are what end users and client applications discover and consume.

This mirrors the approach used in the CHAP Modelling Platform, where generic model template YAMLs are distinguished from specific initialised instances.

---

## 6. Functional requirements

### 6.1 Data pipeline

- Allow EO data to be requested through a unified API where download, processing, and optional upload to DHIS2 happen behind the scenes.
- Each step in the pipeline is also available as a separate API endpoint with clear input and output definitions: data extraction, aggregation, and upload to DHIS2.
- Support scheduling — data can be downloaded, processed, and imported at fixed user-defined intervals.
- Support orchestration — users can compose custom data pipelines, including pre- and post-processing steps.

### 6.2 Data storage and serving

- Store all datasets as GeoZarr — cloud-native, chunked, multiscale, EPSG:4326.
- Expose datasets via a `/zarr/{dataset_id}` endpoint using HTTP range requests, enabling chunk-level access by any compatible client.
- Expose datasets through OGC API-compliant endpoints (Coverages, EDR, Processes, Tiles, Collections) via pygeoapi under `/ogcapi`.
- Expose dataset discovery metadata via a `/datasets` endpoint and a STAC catalogue.

### 6.3 Visualisation

- Support on-the-fly map tile rendering with custom styling — replacing the current Google Earth Engine tiling workflow.
- Support image tile generation using TiTiler (following the OGC API — Tiles specification as closely as possible).
- Support direct browser rendering of Zarr data via zarr-layer (MapLibre custom layer) with GPU reprojection from EPSG:4326 to Spherical Mercator and client-side dynamic colour classification.
- Support point queries (single location time series) for preview before import.

### 6.4 Aggregation

- Aggregate raster data to org unit polygons (or any GeoJSON feature collection).
- Support async execution for long-running aggregation jobs (OGC API Processes pattern).
- Support demographic disaggregation for WorldPop data (age/sex bands as additional Zarr dimensions).

### 6.5 Integration

- Upload aggregated data values directly to DHIS2 (optional — can be skipped for standalone use).
- Accept GeoJSON features from external sources (not only DHIS2 org units).
- Output results as DHIS2 data values, CSV, or JSON.
- Provide a client library / SDK for programmatic access by DHIS2 apps and third-party tools.

### 6.6 Non-functional requirements

- Handle simultaneous and long-running requests without blocking.
- Follow DPG requirements and FAIR principles.
- Build on existing open-source solutions — the team is small and sustainability matters.
- Support deployment via Docker for local, cloud-hosted, and sovereign country environments.
- Storage backend configurable via environment variables — no code changes required to switch between local filesystem, different cloud providers, AWS S3 (including Africa and Asia regions), and self-hosted Ceph/RGW for sovereign deployments.

---

## 7. Supported data sources

The Climate API ingests data from multiple upstream Earth Observation and climate sources. Current and planned sources include:

- **CHIRPS** (Climate Hazards Group InfraRed Precipitation with Station data) — daily and pentadal precipitation.
- **ERA5 / Climate Data Store** (Copernicus CDS) — temperature, humidity, wind, and other atmospheric variables at multiple temporal resolutions.
- **WorldPop** — annual gridded population estimates, with optional age and sex disaggregation at 5-year intervals.

The dataset ID schema encodes the source, variable, period type, and spatial extent ID. The extent ID is an ISO country code or named sub-national identifier defined in the instance configuration. Examples:

- `chirps3_precipitation_daily_sle`
- `era5_temperature_daily_sle`
- `worldpop_population_yearly_sle`

Sub-national extents use the same schema (e.g. `chirps3_precipitation_daily_bo` for the Bo district of Sierra Leone), allowing larger countries to configure a district-level extent to limit initial download volume.

---

## 8. Technical architecture

### 8.1 API layer

The API is built on FastAPI and exposes the following endpoint groups:

| Endpoint             | Description                                                                                                                                                                                                            |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/ingestion`         | Trigger data download from an upstream source for the configured extent and date range. Parameters: `dataset_id`, `start`, `end`, `extent_id`. Creates or updates the corresponding Zarr store.                        |
| `/sync`              | Check for more recent data from the upstream source and append new time steps to the existing Zarr store. Validates temporal continuity before writing.                                                                |
| `/datasets`          | List and describe available published datasets — metadata, period type, extent, last updated, and access links.                                                                                                        |
| `/zarr/{dataset_id}` | Serve the GeoZarr store via HTTP range requests (FastAPI StaticFiles). Consumed by `xarray.open_zarr()`, QGIS, zarr-layer, and the OGC layer. In cloud deployments, redirects to the object storage endpoint directly. |
| `/ogcapi/...`        | OGC API-compliant endpoints served by pygeoapi: Coverages, EDR, Processes, Tiles, Collections.                                                                                                                         |

### 8.2 Storage layer

All datasets are stored as GeoZarr. Key properties:

- EPSG:4326 coordinate reference system.
- CF-compliant coordinate attributes and `_ARRAY_DIMENSIONS` metadata.
- Multiscale pyramid overview levels declared under the `multiscales` key in `.zattrs` — required for efficient zoom-level-aware chunk fetching by zarr-layer.
- Chunk shape tuned per dataset to balance three access patterns: time series queries, polygon aggregation, and browser tile rendering.
- Blosc/Zstd compression.

The storage backend is abstracted via fsspec, enabling the following backends with environment-variable configuration only:

| Backend                  | Notes                                                                                                                                                                      |
| ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Local filesystem         | Default for development. `STORAGE_BACKEND=file`                                                                                                                            |
| European S3-compatible   | Hetzner, Scaleway, IONOS, OVHcloud. GDPR-native. `STORAGE_BACKEND=s3` + `endpoint_url`.                                                                                    |
| AWS S3 (af-south-1)      | Cape Town region — lowest latency for Southern/Eastern Africa deployments.                                                                                                 |
| AWS S3 (ap-southeast-1)  | Singapore — lowest latency for Laos, Sri Lanka, and Southeast Asia deployments.                                                                                            |
| Ceph / RGW (self-hosted) | S3-compatible. For sovereign deployments requiring data to remain within national borders. Runs on university and research network infrastructure (AfricaConnect / GÉANT). |

### 8.3 Data pipeline model

The Climate API follows an ETL (Extract, Transform, Load) pattern — transformation occurs on the processing server before data is loaded into DHIS2. An ELT approach (transformation in a cloud data warehouse) may be supported in a future version.

The pipeline stages are:

- **Extract** — download raw data from the upstream source for the configured extent and time range.
- **Transform** — reproject, rechunk, apply temporal aggregation (if needed), compute derived variables, and write to GeoZarr.
- **Load** — aggregate to org unit polygons and upload data values to DHIS2, or output as CSV/JSON for standalone use.

Each stage is independently accessible as an API endpoint, allowing custom pipelines to be constructed by combining steps in different sequences.

Long-running jobs (ingestion, sync, aggregation) are executed asynchronously via a Celery task queue backed by Redis. This ensures the API remains responsive under concurrent load. Dask is used for parallel computation within each job, processing Zarr chunks concurrently across CPU cores or threads.

### 8.4 Technology stack

| Technology                    | Role in the Climate API                                                                                                                                                                  |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| FastAPI (Python)              | Core REST API framework. Handles ingestion, sync, dataset, and OGC endpoints. Each pipeline step is exposed as a separate endpoint.                                                      |
| Xarray + Zarr                 | In-memory dataset model and cloud-native chunked storage format. GeoZarr conventions applied for geospatial metadata and multiscale pyramid support.                                     |
| Dask                          | Parallel computation within jobs — processes Zarr chunks concurrently for aggregation, reprojection, and derived variable computation. Works natively with Xarray.                       |
| Celery + Redis                | Job queue for concurrent and long-running requests. Celery dispatches jobs across workers; Dask parallelises computation within each job.                                                |
| rioxarray                     | Raster operations on Xarray datasets — reprojection, clipping, resampling, and CRS management.                                                                                           |
| exactextract                  | Polygon aggregation (zonal statistics) to org unit features. Supports weighted partial-pixel coverage for accurate population aggregation.                                               |
| xarray-multiscale             | Generates multiscale pyramid overview levels at ingest time, required for zarr-layer zoom-level-aware chunk fetching.                                                                    |
| rechunker                     | Reshapes existing Zarr stores to a new chunk layout without full rewrite. Used for per-dataset chunk shape tuning.                                                                       |
| cf-xarray                     | CF convention handling — maps standard dimension names and attributes across source datasets.                                                                                            |
| numba                         | JIT compilation for custom processing functions (e.g. consecutive rainy days, heat index) applied pixel-wise over large arrays.                                                          |
| pygeoapi                      | OGC API standards exposure (Coverages, EDR, Processes, Tiles, Collections). Mounted under `/ogcapi`.                                                                                     |
| TiTiler                       | On-the-fly raster tile server. Serves map tiles with dynamic styling, following OGC API - Tiles specification.                                                                           |
| fsspec                        | Unified filesystem abstraction for storage backends (local, S3-compatible, Azure Blob, GCS, Ceph/RGW). Backend is environment-variable configuration only.                               |
| zarr-layer (MapLibre)         | TypeScript library for rendering Zarr directly as a native MapLibre Custom Layer in the browser. GPU reprojection from EPSG:4326 to Spherical Mercator; uses multiscale levels per zoom. |
| Docker                        | Containerised deployment. Supports local, cloud-hosted, and country sovereign deployments.                                                                                               |
| dhis2eo + dhis2-python-client | Underlying Python libraries for data extraction and DHIS2 integration. The Climate API is a no-code layer built on top of these libraries.                                               |
| STAC                          | Complementary discovery and metadata catalogue layer. Each dataset exposed as a STAC Item with temporal, spatial, and access metadata.                                                   |

---

## 9. Standards compliance

The Climate API is designed to be standards-compliant and interoperable. Key standards:

- **OGC API — Coverages**: raw grid access and subsetting.
- **OGC API — EDR** (Environmental Data Retrieval): point and area time series queries.
- **OGC API — Processes**: async zonal aggregation execution.
- **OGC API — Tiles and Maps**: raster tile serving with dynamic styling.
- **OGC API — Collections**: unified dataset discovery.
- **GeoZarr specification**: geospatial metadata conventions for Zarr stores.
- **STAC** (SpatioTemporal Asset Catalog): dataset discovery and asset linking.
- **CF Conventions**: coordinate metadata for Xarray/Zarr datasets.
- **FAIR principles**: datasets are Findable (STAC + `/datasets`), Accessible (open HTTP range requests), Interoperable (OGC APIs + standard formats), and Reusable (documented metadata and provenance).

---

## 10. Deployment and sovereignty

The Climate API is distributed as a Docker image and can be deployed in several configurations:

- **Hosted by HISP Centre** — a centrally managed instance for demo purposes.
- **Country-hosted** — deployed within a country's own infrastructure, with local storage or the nearest available regional cloud provider (AWS af-south-1 for Africa, AWS ap-southeast-1 for Southeast Asia).
- **Sovereign** — deployed on local or research network infrastructure. Data never leaves national borders. Suitable for countries with data residency requirements.

The storage backend is configured entirely via environment variables — no code changes are required to switch between backends. This ensures the same Docker image can be deployed across all contexts.

---

## 11. Related work and repositories

| Resource                 | Link / Description                                                        |
| ------------------------ | ------------------------------------------------------------------------- |
| DHIS2 climate data       | https://dhis2.org/climate/climate-data/                                   |
| CHAP Modelling Platform  | https://chap.dhis2.org/                                                   |
| dhis2eo (Python library) | Underlying extraction and processing library for EO data.                 |
| dhis2-python-client      | DHIS2 API client library used for org unit queries and data value upload. |
| Climate API GitHub       | https://github.com/dhis2/eo-api                                           |
| GeoZarr roadmap          | https://geozarr.org/roadmap.html                                          |
| pygeoapi                 | https://pygeoapi.io/                                                      |
