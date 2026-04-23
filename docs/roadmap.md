# Roadmap

## Step 1 — Foundation (target: DAC 2026)

The first step delivers a working foundation that country teams and tool developers can build on. The scope is deliberately narrow: ingest data, keep it current, store it efficiently, and make it discoverable.

**Ingestion and sync**

- Ingest climate and Earth Observation datasets for a configured spatial extent
- Keep datasets up to date
- Support the built-in dataset catalogue (CHIRPS, ERA5, WorldPop)

**GeoZarr storage**

- Store all datasets as GeoZarr stores
- Multiscale pyramid support for efficient browser-based rendering
- Local filesystem storage; S3-compatible object storage for cloud deployments

**STAC catalogue**

- STAC endpoint for dataset discovery
- Collection-level assets with xarray and datacube extensions
- Enables direct `xarray` and `stackstac` access without API knowledge

**Primary consumers**

- [DHIS2 Climate Tools](https://github.com/dhis2/climate-app) — direct GeoZarr integration
- Any tool with native Zarr or STAC support

---

## Step 2 — Data processing and DHIS2 integration (autumn 2026)

The second step adds the ability to derive new datasets from ingested data and connect the output to DHIS2.

**Data processing**

- Compute derived variables: climate normals, anomalies, exposure indices
- We will investigate whether OGC API Processes and/or openEO is the right fit for process execution and chaining

**DHIS2 integration**

- Spatial aggregation using DHIS2 org unit boundaries
- Push aggregated climate values into DHIS2 data elements against org unit hierarchies

---

## Step 3 — Workflows and orchestration

The third step adds automation so that datasets and derived products stay current without manual intervention.

**Workflows**

- Event-driven cascades: a sync that updates a base dataset triggers downstream derived products automatically
- Scheduled jobs for recurring operations (monthly normals, weekly anomaly updates)
- Integration with external orchestration tools via the standard API surface
