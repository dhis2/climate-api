# ADR 0001: EO API as Artifact Plane and Publication Plane

## Status

Accepted for `pygeoapi-publication-slice`

## Date

2026-03-25

## Context

This branch intentionally simplifies the architecture relative to the parked generic workflow branch.

The immediate goal is to make EO API a practical one-stop service for HISP countries:

1. fetch EO datasets from upstream sources
2. store them inside EO API-managed infrastructure
3. expose them through standards-facing OGC routes
4. avoid requiring downstream users to keep going back to remote upstream sources

The longer-term goal is stronger than "download and re-publish":

1. EO API should become the managed home for EO artifacts
2. raw EO artifacts should remain accessible in their native form
3. standards-facing publication should sit on top of those managed artifacts
4. over time, EO API should reduce dependence on external platforms for routine EO access patterns

That makes one principle especially important:

> Zarr must be respected as Zarr.

A Zarr store is not just a file to zip up for convenience downloads.
If EO API is going to become a serious EO serving platform, native artifact form matters.

At the same time, this branch should not overbuild custom OGC behavior.
During this phase, the project should rely on OGC API via `pygeoapi` rather than reimplementing standards-facing collection and coverage behavior in native FastAPI.

## Decision

EO API will be treated as two aligned planes over the same managed data:

1. **Artifact plane**
   - EO API stores and manages raw EO artifacts
   - artifacts remain accessible in their native form
   - Zarr stays Zarr
   - NetCDF stays NetCDF

2. **Publication plane**
   - EO API publishes managed artifacts through OGC-facing routes
   - `pygeoapi` is the primary standards implementation for this branch
   - `/ogcapi` is the canonical standards-facing path

These are not separate systems with separate truth.
They are two access modes over the same internally managed EO assets.

## Consequences

### What EO API should do

1. download and persist EO datasets locally
2. prefer Zarr where it is the right operational format
3. expose artifact metadata and native artifact identity through FastAPI
4. auto-publish eligible artifacts through `pygeoapi`
5. keep `/artifacts` as a real product surface, not just bookkeeping

### What EO API should not do

1. treat zipped Zarr as the canonical raw artifact representation
2. make `pygeoapi` the only way to access stored EO data
3. require feature-based selection as the primary download model for gridded EO data
4. reintroduce the generic component/workflow/publication platform before the simpler vertical slice is stable

## Native Artifact Principle

Raw artifact access is a first-class responsibility.

For example:

1. a NetCDF artifact may be served directly as a file
2. a Zarr artifact should remain identifiable as a Zarr store
3. a zipped Zarr export may exist as a convenience, but not as the canonical artifact form

This matters because future Zarr-aware clients should be able to treat EO API as a real data source, not just a place to download repackaged exports.

## OGC / pygeoapi Principle

This branch should rely on OGC API through `pygeoapi` while the vertical slice is being established.

That means:

1. `/ogcapi` remains the public standards-facing path
2. `pygeoapi` is used for collection and coverage-oriented publication
3. FastAPI remains the control plane and artifact plane
4. native FastAPI should avoid rebuilding generic OGC collection behavior unless a clear product need emerges

In short:

1. FastAPI manages artifacts
2. `pygeoapi` publishes them
3. both belong to EO API's serving story

## Implications For Current Scope

Phase 1 should focus on:

1. upstream download
2. local artifact persistence
3. auto-publication into `/ogcapi`
4. stable artifact metadata and lookup
5. extent-first and country-first download inputs for gridded datasets

Later phases may add:

1. native Zarr-store HTTP serving
2. richer publication refresh behavior
3. pipelines and orchestration
4. local artifact import/publication
5. more advanced compute-oriented access patterns

## Short Statement

Use this as the branch summary:

> EO API should become the managed home for EO data, not just a thin proxy to remote sources. Raw artifacts remain first-class in EO API, and OGC publication through pygeoapi is the standards-facing layer built on top of those managed artifacts.
