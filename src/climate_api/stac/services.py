"""STAC catalogue builders backed by published artifact records."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pystac
from fastapi import HTTPException, Request
from xstac import xarray_to_stac

from climate_api.data_accessor.services.accessor import open_zarr_dataset
from climate_api.data_manager.services.utils import get_lon_lat_dims, get_time_dim
from climate_api.data_registry.services import datasets as registry_datasets
from climate_api.ingestions import services as ingestion_services
from climate_api.ingestions.schemas import ArtifactFormat, ArtifactRecord, PublicationStatus
from climate_api.shared.time import parse_period_string_to_datetime

CATALOG_ID = "climate-api"
CATALOG_TITLE = "DHIS2 Climate API"
CATALOG_DESCRIPTION = "Published Climate API GeoZarr datasets"
STAC_VERSION = "1.1.0"
DATACUBE_EXTENSION = "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
ZARR_EXTENSION = "https://stac-extensions.github.io/zarr/v1.1.0/schema.json"
DEFAULT_STAC_LICENSE = "various"
SPATIAL_STEP_DECIMALS = 8
logger = logging.getLogger(__name__)


def build_catalog(request: Request) -> dict[str, object]:
    """Build the STAC catalog document."""
    self_href = str(request.url)
    catalog_href = _abs_url(request, "/stac/catalog.json")
    links = [
        {"rel": "self", "href": self_href, "type": "application/json"},
        {"rel": "root", "href": catalog_href, "type": "application/json"},
    ]
    for dataset_id, artifact in _eligible_artifacts_by_dataset().items():
        links.append(
            {
                "rel": "child",
                "href": _abs_url(request, f"/stac/collections/{dataset_id}"),
                "title": artifact.dataset_name,
                "type": "application/json",
            }
        )
    return {
        "stac_version": STAC_VERSION,
        "type": "Catalog",
        "id": CATALOG_ID,
        "title": CATALOG_TITLE,
        "description": CATALOG_DESCRIPTION,
        "links": links,
    }


def build_collection(dataset_id: str, request: Request) -> dict[str, object]:
    """Build one STAC collection document."""
    artifact = _eligible_artifacts_by_dataset().get(dataset_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"STAC collection '{dataset_id}' not found")

    source_dataset = registry_datasets.get_dataset(artifact.dataset_id) or {}
    collection_href = _abs_url(request, f"/stac/collections/{dataset_id}")
    catalog_href = _abs_url(request, "/stac/catalog.json")
    dataset_href = _abs_url(request, f"/datasets/{dataset_id}")
    zarr_href = _public_zarr_asset_href(request, dataset_id, artifact, source_dataset)

    template = _build_collection_template(
        dataset_id=dataset_id,
        artifact=artifact,
        collection_href=collection_href,
        catalog_href=catalog_href,
        dataset_href=dataset_href,
        zarr_href=zarr_href,
        source_dataset=source_dataset,
    )
    template_links = [_link_to_dict(link) for link in template.links]

    collection_payload = _build_collection_with_xstac(artifact=artifact, template=template)
    collection_payload["id"] = dataset_id
    collection_payload["type"] = "Collection"
    collection_payload["stac_version"] = STAC_VERSION
    collection_payload["description"] = template.description
    collection_payload["title"] = template.title
    existing_extensions = collection_payload.get("stac_extensions", [])
    if isinstance(existing_extensions, list):
        collection_payload["stac_extensions"] = sorted({*existing_extensions, DATACUBE_EXTENSION, ZARR_EXTENSION})
    else:
        collection_payload["stac_extensions"] = sorted([DATACUBE_EXTENSION, ZARR_EXTENSION])
    collection_payload["links"] = template_links
    assets = collection_payload.setdefault("assets", {})
    zarr_from_xstac = assets.get("zarr", {}) if isinstance(assets, dict) else {}
    template_asset = _asset_to_dict(_required_zarr_asset(template))
    xarray_open_kwargs = _zarr_open_kwargs(artifact)
    collection_payload["assets"]["zarr"] = {
        **zarr_from_xstac,
        **_zarr_asset_metadata(artifact),
        "href": template_asset["href"],
        "type": template_asset.get("type"),
        "title": template_asset.get("title"),
        "roles": template_asset.get("roles"),
        "xarray:open_kwargs": xarray_open_kwargs,
    }
    collection_payload["license"] = template.license
    _remove_helper_variables(collection_payload)
    _round_spatial_steps(collection_payload)
    _override_time_step(collection_payload, _period_step(source_dataset.get("period_type")))
    _override_spatial_extent_from_artifact(collection_payload, artifact)
    _override_temporal_extent_from_artifact(collection_payload, artifact)
    _sanitize_variable_attrs(collection_payload)
    return collection_payload


def _eligible_artifacts_by_dataset() -> dict[str, ArtifactRecord]:
    result: dict[str, ArtifactRecord] = {}
    for dataset_id, artifacts in ingestion_services.group_datasets().items():
        latest = max(artifacts, key=lambda artifact: artifact.created_at)
        if latest.publication.status != PublicationStatus.PUBLISHED:
            continue
        if latest.format != ArtifactFormat.ZARR:
            continue
        result[dataset_id] = latest
    return dict(sorted(result.items()))


def _build_collection_template(
    *,
    dataset_id: str,
    artifact: ArtifactRecord,
    collection_href: str,
    catalog_href: str,
    dataset_href: str,
    zarr_href: str,
    source_dataset: dict[str, Any],
) -> pystac.Collection:
    spatial = artifact.coverage.spatial
    temporal = artifact.coverage.temporal
    template = pystac.Collection(
        id=dataset_id,
        description=f"Published GeoZarr dataset for {artifact.dataset_name}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[spatial.xmin, spatial.ymin, spatial.xmax, spatial.ymax]]),
            temporal=pystac.TemporalExtent(
                [[parse_period_string_to_datetime(temporal.start), parse_period_string_to_datetime(temporal.end)]]
            ),
        ),
        title=artifact.dataset_name,
        stac_extensions=[
            DATACUBE_EXTENSION,
            ZARR_EXTENSION,
        ],
        license=DEFAULT_STAC_LICENSE,
    )
    template.extra_fields["keywords"] = _keywords(artifact, source_dataset)
    template.clear_links()
    template.add_link(pystac.Link(rel="self", target=collection_href, media_type="application/json"))
    template.add_link(pystac.Link(rel="root", target=catalog_href, media_type="application/json"))
    template.add_link(pystac.Link(rel="parent", target=catalog_href, media_type="application/json"))
    template.add_link(
        pystac.Link(rel="alternate", target=dataset_href, media_type="application/json", title="Dataset detail")
    )
    template.add_asset(
        "zarr",
        pystac.Asset(
            href=zarr_href,
            media_type="application/vnd+zarr",
            title="Zarr store",
            roles=["data"],
        ),
    )
    return template


def _build_collection_with_xstac(*, artifact: ArtifactRecord, template: pystac.Collection) -> dict[str, Any]:
    try:
        ds = open_zarr_dataset(_artifact_store_path(artifact))
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to open published Zarr store for artifact '%s'", artifact.artifact_id)
        raise HTTPException(
            status_code=503,
            detail=f"Published Zarr store for artifact '{artifact.artifact_id}' is temporarily unavailable",
        ) from exc
    try:
        x_dimension, y_dimension = get_lon_lat_dims(ds)
        time_dimension = get_time_dim(ds)
        result = xarray_to_stac(
            ds,
            template,
            temporal_dimension=time_dimension,
            x_dimension=x_dimension,
            y_dimension=y_dimension,
            reference_system=4326,
            # Schema validation can trigger outbound fetches for STAC extension schemas.
            validate=False,
        )
        # build_collection replaces links from the template after xstac runs, so
        # clear xstac/pystac-owned links before serialization to avoid root-link
        # resolution attempts during to_dict().
        result.clear_links()
        payload: dict[str, Any] = result.to_dict(include_self_link=False)
        return payload
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to derive STAC metadata from artifact '%s'", artifact.artifact_id)
        raise HTTPException(
            status_code=503,
            detail=f"Published Zarr store for artifact '{artifact.artifact_id}' is temporarily unavailable",
        ) from exc
    finally:
        ds.close()


def _link_to_dict(link: pystac.Link) -> dict[str, Any]:
    target = link.target
    href = target if isinstance(target, str) else link.href
    payload = {"rel": link.rel, "href": str(href)}
    if link.media_type is not None:
        payload["type"] = link.media_type
    if link.title is not None:
        payload["title"] = link.title
    return payload


def _asset_to_dict(asset: pystac.Asset) -> dict[str, Any]:
    payload: dict[str, Any] = {"href": asset.href}
    if asset.media_type is not None:
        payload["type"] = asset.media_type
    if asset.title is not None:
        payload["title"] = asset.title
    if asset.roles is not None:
        payload["roles"] = asset.roles
    payload.update(asset.extra_fields)
    return payload


def _required_zarr_asset(template: pystac.Collection) -> pystac.Asset:
    asset = template.assets.get("zarr")
    if asset is None:
        raise HTTPException(status_code=500, detail="STAC template is missing the required zarr asset")
    return asset


def _artifact_store_path(artifact: ArtifactRecord) -> str:
    if artifact.path:
        return artifact.path
    if artifact.asset_paths:
        return artifact.asset_paths[0]
    raise HTTPException(
        status_code=500,
        detail=f"Published artifact '{artifact.artifact_id}' has no readable storage path metadata",
    )


def _public_zarr_asset_href(
    request: Request,
    dataset_id: str,
    artifact: ArtifactRecord,
    source_dataset: dict[str, Any],
) -> str:
    cache_info = source_dataset.get("cache_info")
    # Use dataset-template multiscale metadata so local and remote stores map to
    # the same public href contract without filesystem probing.
    if isinstance(cache_info, dict) and cache_info.get("multiscales"):
        return _abs_url(request, f"/zarr/{dataset_id}/0")
    return _abs_url(request, f"/zarr/{dataset_id}")


def _abs_url(request: Request, path: str) -> str:
    base_url = os.getenv("CLIMATE_API_BASE_URL")
    if base_url:
        return f"{base_url.rstrip('/')}{path}"
    return f"{str(request.base_url).rstrip('/')}{path}"


def _period_step(period_type: object) -> str | None:
    if period_type == "hourly":
        return "PT1H"
    if period_type == "daily":
        return "P1D"
    if period_type == "monthly":
        return "P1M"
    if period_type == "yearly":
        return "P1Y"
    return None


def _override_time_step(collection: dict[str, Any], step: str | None) -> None:
    if step is None:
        return
    dimensions = collection.setdefault("cube:dimensions", {})
    for key, value in dimensions.items():
        if isinstance(value, dict) and value.get("type") == "temporal":
            value["step"] = step
            dimensions[key] = value
            return


def _round_spatial_steps(collection: dict[str, Any]) -> None:
    dimensions = collection.get("cube:dimensions")
    if not isinstance(dimensions, dict):
        return
    for key, value in dimensions.items():
        if not isinstance(value, dict) or value.get("type") != "spatial":
            continue
        step = value.get("step")
        if isinstance(step, int | float):
            value["step"] = round(float(step), SPATIAL_STEP_DECIMALS)
            dimensions[key] = value


def _override_spatial_extent_from_artifact(collection: dict[str, Any], artifact: ArtifactRecord) -> None:
    spatial = artifact.coverage.spatial
    collection["extent"]["spatial"]["bbox"] = [[spatial.xmin, spatial.ymin, spatial.xmax, spatial.ymax]]


def _override_temporal_extent_from_artifact(collection: dict[str, Any], artifact: ArtifactRecord) -> None:
    temporal = artifact.coverage.temporal
    start = parse_period_string_to_datetime(temporal.start).isoformat().replace("+00:00", "Z")
    end = parse_period_string_to_datetime(temporal.end).isoformat().replace("+00:00", "Z")
    collection["extent"]["temporal"]["interval"] = [
        [
            start,
            end,
        ]
    ]
    dimensions = collection.setdefault("cube:dimensions", {})
    for key, value in dimensions.items():
        if isinstance(value, dict) and value.get("type") == "temporal":
            value["extent"] = [start, end]
            dimensions[key] = value
            return


def _sanitize_variable_attrs(collection: dict[str, Any]) -> None:
    variables = collection.get("cube:variables")
    if not isinstance(variables, dict):
        return
    for _, variable in variables.items():
        if not isinstance(variable, dict):
            continue
        attrs = variable.get("attrs")
        if not isinstance(attrs, dict):
            continue
        kept_attrs: dict[str, str] = {}
        long_name = attrs.get("long_name")
        units = attrs.get("units")
        if isinstance(long_name, str):
            kept_attrs["long_name"] = long_name
        if isinstance(units, str):
            kept_attrs["units"] = units
            variable["unit"] = units
        variable["attrs"] = kept_attrs


def _remove_helper_variables(collection: dict[str, Any]) -> None:
    variables = collection.get("cube:variables")
    if not isinstance(variables, dict):
        return
    for key in list(variables):
        variable = variables.get(key)
        if not isinstance(variable, dict):
            continue
        dimensions = variable.get("dimensions")
        # xstac can emit scalar CRS/grid-mapping helper variables with no dimensions.
        if isinstance(dimensions, list) and len(dimensions) == 0:
            variables.pop(key, None)


def _keywords(artifact: ArtifactRecord, source_dataset: dict[str, Any]) -> list[str]:
    keywords = [artifact.dataset_id, artifact.variable, "zarr", "stac"]
    for key in ("source", "short_name"):
        value = source_dataset.get(key)
        if isinstance(value, str) and value:
            keywords.append(value)
    return keywords


def _zarr_asset_metadata(artifact: ArtifactRecord) -> dict[str, object]:
    metadata: dict[str, object] = {"zarr:node_type": "group"}
    artifact_path = _artifact_store_path(artifact)
    consolidated = _zarr_consolidated_flag(artifact_path)
    if consolidated is not None:
        metadata["zarr:consolidated"] = consolidated
    if "://" in artifact_path:
        return metadata
    store_root = Path(artifact_path)
    zarr_json = store_root / "zarr.json"
    if zarr_json.exists():
        metadata["zarr:zarr_format"] = 3
    else:
        zgroup = store_root / ".zgroup"
        if zgroup.exists():
            metadata["zarr:zarr_format"] = 2
    return metadata


def _zarr_open_kwargs(artifact: ArtifactRecord) -> dict[str, bool | None]:
    return {"consolidated": _zarr_consolidated_flag(_artifact_store_path(artifact))}


def _zarr_consolidated_flag(artifact_path: str) -> bool | None:
    if "://" in artifact_path:
        return None

    store_root = Path(artifact_path)
    zarr_json = store_root / "zarr.json"
    if zarr_json.exists():
        try:
            payload = json.loads(zarr_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return "consolidated_metadata" in payload

    if (store_root / ".zmetadata").exists():
        return True
    if (store_root / ".zgroup").exists():
        return False
    return None
