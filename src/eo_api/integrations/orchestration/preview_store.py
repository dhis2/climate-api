"""Backend store for generic DHIS2 preview collection rows."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

_PREVIEW_COLLECTION_ID = "generic-dhis2-datavalue-preview"
_PREVIEW_PG_TABLE = "generic_dhis2_datavalue_preview"
_PREVIEW_COLLECTION_PATH = Path(
    os.getenv("GENERIC_DHIS2_DATAVALUE_PREVIEW_PATH", "/tmp/generic_dhis2_datavalue_preview.geojson")
)
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _empty_feature_collection() -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(lambda: asyncio.run(coro)).result()


def _validate_table_name(table_name: str) -> str:
    if not _TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid PostgreSQL table name: {table_name}")
    return table_name


def _pg_dsn() -> str:
    return os.getenv("EO_API_PG_DSN", "").strip()


def _preview_ttl_days() -> int:
    raw = os.getenv("EO_API_PREVIEW_TTL_DAYS", "90").strip()
    try:
        value = int(raw)
    except ValueError:
        return 90
    return value if value > 0 else 90


def _cleanup_on_startup_enabled() -> bool:
    raw = os.getenv("EO_API_PREVIEW_CLEANUP_ON_STARTUP", "true").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _as_feature(feature_id: str, properties: dict[str, Any], geometry: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry,
        "properties": properties,
    }


_PREFERRED_PROPERTY_ORDER = [
    "orgUnit",
    "orgUnitName",
    "period",
    "value",
    "dataElement",
    "categoryOptionCombo",
    "attributeOptionCombo",
    "job_id",
    "dataset_type",
    "published_at",
]


def _order_preview_properties(properties: dict[str, Any]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}
    for key in _PREFERRED_PROPERTY_ORDER:
        if key in properties:
            ordered[key] = properties[key]
    for key, value in properties.items():
        if key not in ordered:
            ordered[key] = value
    return ordered


def _decode_jsonb_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _asyncpg_module() -> Any:
    import asyncpg

    return asyncpg


def _use_postgres_backend() -> bool:
    return bool(_pg_dsn())


def ensure_preview_store_seeded(*, file_path: Path | None = None) -> str:
    """Ensure configured preview backend is initialized."""
    if _use_postgres_backend():
        _run_async(_ensure_postgres_store_seeded())
        return _validate_table_name(_PREVIEW_PG_TABLE)
    path = file_path or _PREVIEW_COLLECTION_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(json.dumps(_empty_feature_collection()), encoding="utf-8")
    return str(path)


def publish_preview_rows(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
    geometry_by_org_unit: dict[str, dict[str, Any]] | None = None,
    file_path: Path | None = None,
) -> dict[str, Any]:
    """Publish preview rows to the configured backend."""
    if _use_postgres_backend():
        result = _run_async(
            _publish_preview_rows_postgres(
                dataset_type=dataset_type,
                rows=rows,
                job_id=job_id,
                geometry_by_org_unit=geometry_by_org_unit,
            )
        )
        return cast(dict[str, Any], result)
    return _publish_preview_rows_file(
        dataset_type=dataset_type,
        rows=rows,
        job_id=job_id,
        geometry_by_org_unit=geometry_by_org_unit,
        file_path=file_path,
    )


def load_preview_features(*, job_id: str | None = None, file_path: Path | None = None) -> list[dict[str, Any]]:
    """Load preview features from configured backend."""
    if _use_postgres_backend():
        result = _run_async(_load_preview_features_postgres(job_id=job_id))
        return cast(list[dict[str, Any]], result)
    return _load_preview_features_file(job_id=job_id, file_path=file_path)


def get_latest_preview_job_id(*, file_path: Path | None = None) -> str | None:
    """Return the most recently published preview job id."""
    if _use_postgres_backend():
        result = _run_async(_get_latest_preview_job_id_postgres())
        return cast(str | None, result)
    return _get_latest_preview_job_id_file(file_path=file_path)


def list_preview_jobs(
    *,
    limit: int = 50,
    dataset_type: str | None = None,
    file_path: Path | None = None,
) -> list[dict[str, Any]]:
    """List preview jobs ordered by most recent publication time."""
    effective_limit = max(1, limit)
    normalized_dataset = (dataset_type or "").strip().lower() or None
    if _use_postgres_backend():
        result = _run_async(_list_preview_jobs_postgres(limit=effective_limit, dataset_type=normalized_dataset))
        return cast(list[dict[str, Any]], result)
    return _list_preview_jobs_file(limit=effective_limit, dataset_type=normalized_dataset, file_path=file_path)


def list_preview_periods(*, job_id: str, file_path: Path | None = None) -> list[str]:
    """List available periods for one preview job, newest first."""
    if _use_postgres_backend():
        result = _run_async(_list_preview_periods_postgres(job_id=job_id))
        return cast(list[str], result)
    return _list_preview_periods_file(job_id=job_id, file_path=file_path)


def query_preview_features(
    *,
    job_id: str | None = None,
    offset: int = 0,
    limit: int = 10,
    file_path: Path | None = None,
) -> dict[str, Any]:
    """Query preview features with backend-aware pagination pushdown."""
    effective_offset = max(offset, 0)
    effective_limit = max(limit, 0)
    if _use_postgres_backend():
        result = _run_async(
            _query_preview_features_postgres(
                job_id=job_id,
                offset=effective_offset,
                limit=effective_limit,
            )
        )
        return cast(dict[str, Any], result)
    return _query_preview_features_file(
        job_id=job_id,
        offset=effective_offset,
        limit=effective_limit,
        file_path=file_path,
    )


def get_preview_feature(identifier: str, *, file_path: Path | None = None) -> dict[str, Any] | None:
    """Fetch a single preview feature by identifier."""
    if _use_postgres_backend():
        result = _run_async(_get_preview_feature_postgres(identifier))
        return cast(dict[str, Any] | None, result)
    return _get_preview_feature_file(identifier, file_path=file_path)


def infer_preview_fields() -> dict[str, dict[str, str]]:
    """Infer schema fields from preview backend."""
    fields: dict[str, dict[str, str]] = {
        "job_id": {"type": "string"},
        "dataset_type": {"type": "string"},
        "orgUnit": {"type": "string"},
        "orgUnitName": {"type": "string"},
        "period": {"type": "string"},
        "value": {"type": "number"},
        "dataElement": {"type": "string"},
    }
    features = load_preview_features()
    if not features:
        return fields

    # Use a small sample to avoid first-row bias in mixed datasets.
    for feature in features[:200]:
        props = feature.get("properties", {})
        if not isinstance(props, dict):
            continue
        for key, value in props.items():
            if isinstance(value, bool):
                ftype = "boolean"
            elif isinstance(value, int):
                ftype = "integer"
            elif isinstance(value, float):
                ftype = "number"
            else:
                ftype = "string"
            fields[str(key)] = {"type": ftype}
    return fields


def cleanup_preview_store(*, ttl_days: int | None = None, file_path: Path | None = None) -> dict[str, Any]:
    """Delete expired preview records older than configured retention window."""
    effective_ttl_days = ttl_days if ttl_days is not None else _preview_ttl_days()
    if _use_postgres_backend():
        deleted = _run_async(_cleanup_preview_store_postgres(ttl_days=effective_ttl_days))
        return {"backend": "postgresql", "deleted_count": int(deleted), "ttl_days": effective_ttl_days}
    deleted = _cleanup_preview_store_file(ttl_days=effective_ttl_days, file_path=file_path)
    return {"backend": "file", "deleted_count": deleted, "ttl_days": effective_ttl_days}


def _publish_preview_rows_file(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
    geometry_by_org_unit: dict[str, dict[str, Any]] | None = None,
    file_path: Path | None = None,
) -> dict[str, Any]:
    path = file_path or _PREVIEW_COLLECTION_PATH
    ensure_preview_store_seeded(file_path=path)

    effective_job_id = job_id or uuid.uuid4().hex
    published_at = datetime.now(UTC).isoformat()
    appended_features = []
    for idx, row in enumerate(rows):
        properties = _order_preview_properties(dict(row))
        properties["dataset_type"] = dataset_type
        properties["job_id"] = effective_job_id
        properties["published_at"] = published_at
        properties = _order_preview_properties(properties)
        org_unit = str(properties.get("orgUnit", ""))
        geometry = geometry_by_org_unit.get(org_unit) if geometry_by_org_unit else None
        appended_features.append(_as_feature(f"{effective_job_id}-{idx}", properties, geometry))

    with path.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.load(handle)
            features = payload.get("features", [])
            if not isinstance(features, list):
                features = []
            features.extend(appended_features)
            payload = {"type": "FeatureCollection", "features": features}
            handle.seek(0)
            json.dump(payload, handle)
            handle.truncate()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    return {
        "collection_id": _PREVIEW_COLLECTION_ID,
        "path": str(path),
        "job_id": effective_job_id,
        "item_count": len(appended_features),
        "total_item_count": len(payload["features"]),
        "backend": "file",
    }


def _load_preview_features_file(*, job_id: str | None = None, file_path: Path | None = None) -> list[dict[str, Any]]:
    path = file_path or _PREVIEW_COLLECTION_PATH
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not isinstance(features, list):
        return []
    if not job_id:
        return features
    return [f for f in features if str((f.get("properties") or {}).get("job_id")) == job_id]


def _get_preview_feature_file(identifier: str, *, file_path: Path | None = None) -> dict[str, Any] | None:
    for feature in _load_preview_features_file(file_path=file_path):
        if str(feature.get("id")) == identifier:
            return feature
    return None


def _query_preview_features_file(
    *,
    job_id: str | None = None,
    offset: int,
    limit: int,
    file_path: Path | None = None,
) -> dict[str, Any]:
    features = _load_preview_features_file(job_id=job_id, file_path=file_path)
    number_matched = len(features)
    page = features[offset : offset + limit]
    return {
        "features": page,
        "numberMatched": number_matched,
        "numberReturned": len(page),
    }


def _get_latest_preview_job_id_file(*, file_path: Path | None = None) -> str | None:
    features = _load_preview_features_file(file_path=file_path)
    by_job: dict[str, tuple[datetime, int]] = {}
    for feature in features:
        props = feature.get("properties")
        if not isinstance(props, dict):
            continue
        job_id_raw = props.get("job_id")
        published_at_raw = props.get("published_at")
        if not isinstance(job_id_raw, str) or not isinstance(published_at_raw, str):
            continue
        try:
            parsed = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        current = by_job.get(job_id_raw)
        if current is None:
            by_job[job_id_raw] = (parsed, 1)
            continue
        latest_seen, count = current
        by_job[job_id_raw] = (parsed if parsed > latest_seen else latest_seen, count + 1)

    if not by_job:
        return None
    # Choose most recent job; tie-break with larger row count then lexical job id.
    ranked = sorted(by_job.items(), key=lambda item: (item[1][0], item[1][1], item[0]), reverse=True)
    return ranked[0][0]


def _list_preview_jobs_file(
    *,
    limit: int,
    dataset_type: str | None = None,
    file_path: Path | None = None,
) -> list[dict[str, Any]]:
    features = _load_preview_features_file(file_path=file_path)
    by_job: dict[str, dict[str, Any]] = {}
    for feature in features:
        props = feature.get("properties")
        if not isinstance(props, dict):
            continue
        job_id_raw = props.get("job_id")
        published_at_raw = props.get("published_at")
        dataset_type_raw = props.get("dataset_type")
        if not isinstance(job_id_raw, str) or not isinstance(published_at_raw, str):
            continue
        if dataset_type and str(dataset_type_raw).strip().lower() != dataset_type:
            continue
        try:
            parsed = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        entry = by_job.get(job_id_raw)
        if entry is None:
            by_job[job_id_raw] = {
                "job_id": job_id_raw,
                "dataset_type": str(dataset_type_raw) if isinstance(dataset_type_raw, str) else None,
                "published_at": parsed,
                "row_count": 1,
            }
            continue
        entry["row_count"] = int(entry["row_count"]) + 1
        if parsed > entry["published_at"]:
            entry["published_at"] = parsed
            if isinstance(dataset_type_raw, str):
                entry["dataset_type"] = dataset_type_raw

    ranked = sorted(
        by_job.values(),
        key=lambda item: (item["published_at"], item["row_count"], item["job_id"]),
        reverse=True,
    )[:limit]
    return [
        {
            "job_id": str(item["job_id"]),
            "dataset_type": item.get("dataset_type"),
            "published_at": cast(datetime, item["published_at"]).isoformat(),
            "row_count": int(item["row_count"]),
        }
        for item in ranked
    ]


def _list_preview_periods_file(*, job_id: str, file_path: Path | None = None) -> list[str]:
    features = _load_preview_features_file(job_id=job_id, file_path=file_path)
    periods = {
        str((feature.get("properties") or {}).get("period"))
        for feature in features
        if isinstance(feature.get("properties"), dict) and (feature.get("properties") or {}).get("period") is not None
    }
    return sorted(periods, reverse=True)


async def _ensure_postgres_store_seeded() -> None:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    conn = await asyncpg.connect(_pg_dsn())
    try:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                feature_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                published_at TIMESTAMPTZ NOT NULL,
                properties JSONB NOT NULL,
                geometry JSONB
            );
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {table_name.replace('.', '_')}_job_id_idx ON {table_name}(job_id);"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {table_name.replace('.', '_')}_dataset_idx ON {table_name}(dataset_type);"
        )
    finally:
        await conn.close()


def _cleanup_preview_store_file(*, ttl_days: int, file_path: Path | None = None) -> int:
    path = file_path or _PREVIEW_COLLECTION_PATH
    if not path.exists():
        return 0

    cutoff = datetime.now(UTC) - timedelta(days=ttl_days)
    deleted_count = 0

    with path.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.load(handle)
            features = payload.get("features", [])
            if not isinstance(features, list):
                features = []
            kept: list[dict[str, Any]] = []
            for feature in features:
                properties = feature.get("properties")
                if not isinstance(properties, dict):
                    kept.append(feature)
                    continue
                published_at_raw = properties.get("published_at")
                if not isinstance(published_at_raw, str):
                    kept.append(feature)
                    continue
                parsed: datetime | None = None
                try:
                    parsed = datetime.fromisoformat(published_at_raw.replace("Z", "+00:00"))
                except ValueError:
                    parsed = None
                if parsed is None:
                    kept.append(feature)
                    continue
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                if parsed < cutoff:
                    deleted_count += 1
                else:
                    kept.append(feature)
            payload = {"type": "FeatureCollection", "features": kept}
            handle.seek(0)
            json.dump(payload, handle)
            handle.truncate()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    return deleted_count


async def _publish_preview_rows_postgres(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
    geometry_by_org_unit: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    effective_job_id = job_id or uuid.uuid4().hex
    published_at = datetime.now(UTC)
    records: list[tuple[str, str, str, datetime, str, str]] = []
    for idx, row in enumerate(rows):
        properties = _order_preview_properties(dict(row))
        properties["dataset_type"] = dataset_type
        properties["job_id"] = effective_job_id
        properties["published_at"] = published_at.isoformat()
        properties = _order_preview_properties(properties)
        org_unit = str(properties.get("orgUnit", ""))
        geometry = geometry_by_org_unit.get(org_unit) if geometry_by_org_unit else None
        records.append(
            (
                f"{effective_job_id}-{idx}",
                effective_job_id,
                dataset_type,
                published_at,
                json.dumps(properties),
                json.dumps(geometry) if isinstance(geometry, dict) else "null",
            )
        )

    conn = await asyncpg.connect(_pg_dsn())
    try:
        if records:
            await conn.executemany(
                f"""
                INSERT INTO {table_name} (feature_id, job_id, dataset_type, published_at, properties, geometry)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
                ON CONFLICT (feature_id) DO UPDATE
                SET job_id = EXCLUDED.job_id,
                    dataset_type = EXCLUDED.dataset_type,
                    published_at = EXCLUDED.published_at,
                    properties = EXCLUDED.properties,
                    geometry = EXCLUDED.geometry;
                """,
                records,
            )
        total_count = int(await conn.fetchval(f"SELECT COUNT(*) FROM {table_name};"))
    finally:
        await conn.close()

    return {
        "collection_id": _PREVIEW_COLLECTION_ID,
        "path": table_name,
        "job_id": effective_job_id,
        "item_count": len(records),
        "total_item_count": total_count,
        "backend": "postgresql",
    }


async def _load_preview_features_postgres(*, job_id: str | None = None) -> list[dict[str, Any]]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    conn = await asyncpg.connect(_pg_dsn())
    try:
        if job_id:
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                WHERE job_id = $1
                ORDER BY published_at DESC, feature_id ASC;
                """,
                job_id,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                ORDER BY published_at DESC, feature_id ASC;
                """
            )
    finally:
        await conn.close()

    return [
        _as_feature(
            str(row["feature_id"]),
            _decode_jsonb_object(row["properties"]),
            _decode_jsonb_object(row["geometry"]) if row["geometry"] is not None else None,
        )
        for row in rows
    ]


async def _get_preview_feature_postgres(identifier: str) -> dict[str, Any] | None:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    conn = await asyncpg.connect(_pg_dsn())
    try:
        row = await conn.fetchrow(
            f"""
            SELECT feature_id, properties, geometry
            FROM {table_name}
            WHERE feature_id = $1;
            """,
            identifier,
        )
    finally:
        await conn.close()

    if row is None:
        return None

    props = _decode_jsonb_object(row["properties"])
    geom = _decode_jsonb_object(row["geometry"]) if row["geometry"] is not None else None
    return _as_feature(str(row["feature_id"]), props, geom)


async def _cleanup_preview_store_postgres(*, ttl_days: int) -> int:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()
    conn = await asyncpg.connect(_pg_dsn())
    try:
        deleted = await conn.fetchval(
            f"DELETE FROM {table_name} WHERE published_at < (NOW() - make_interval(days => $1));",
            ttl_days,
        )
    finally:
        await conn.close()
    return int(deleted or 0)


async def _query_preview_features_postgres(
    *,
    job_id: str | None = None,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()
    conn = await asyncpg.connect(_pg_dsn())
    try:
        if job_id:
            count = int(await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} WHERE job_id = $1;", job_id))
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                WHERE job_id = $1
                ORDER BY published_at DESC, feature_id ASC
                OFFSET $2 LIMIT $3;
                """,
                job_id,
                offset,
                limit,
            )
        else:
            count = int(await conn.fetchval(f"SELECT COUNT(*) FROM {table_name};"))
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                ORDER BY published_at DESC, feature_id ASC
                OFFSET $1 LIMIT $2;
                """,
                offset,
                limit,
            )
    finally:
        await conn.close()

    features = [
        _as_feature(
            str(row["feature_id"]),
            _decode_jsonb_object(row["properties"]),
            _decode_jsonb_object(row["geometry"]) if row["geometry"] is not None else None,
        )
        for row in rows
    ]
    return {
        "features": features,
        "numberMatched": count,
        "numberReturned": len(features),
    }


async def _get_latest_preview_job_id_postgres() -> str | None:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()
    conn = await asyncpg.connect(_pg_dsn())
    try:
        row = await conn.fetchrow(
            f"""
            SELECT job_id
            FROM {table_name}
            GROUP BY job_id
            ORDER BY MAX(published_at) DESC, COUNT(*) DESC, job_id DESC
            LIMIT 1;
            """
        )
    finally:
        await conn.close()
    if row is None:
        return None
    value = row["job_id"]
    return value if isinstance(value, str) and value else None


async def _list_preview_jobs_postgres(
    *,
    limit: int,
    dataset_type: str | None = None,
) -> list[dict[str, Any]]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()
    conn = await asyncpg.connect(_pg_dsn())
    try:
        if dataset_type:
            rows = await conn.fetch(
                f"""
                SELECT
                    job_id,
                    MAX(dataset_type) AS dataset_type,
                    MAX(published_at) AS published_at,
                    COUNT(*) AS row_count
                FROM {table_name}
                WHERE LOWER(dataset_type) = LOWER($1)
                GROUP BY job_id
                ORDER BY MAX(published_at) DESC, COUNT(*) DESC, job_id DESC
                LIMIT $2;
                """,
                dataset_type,
                limit,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT
                    job_id,
                    MAX(dataset_type) AS dataset_type,
                    MAX(published_at) AS published_at,
                    COUNT(*) AS row_count
                FROM {table_name}
                GROUP BY job_id
                ORDER BY MAX(published_at) DESC, COUNT(*) DESC, job_id DESC
                LIMIT $1;
                """,
                limit,
            )
    finally:
        await conn.close()

    result: list[dict[str, Any]] = []
    for row in rows:
        published_at = row["published_at"]
        result.append(
            {
                "job_id": str(row["job_id"]),
                "dataset_type": str(row["dataset_type"]) if row["dataset_type"] is not None else None,
                "published_at": published_at.isoformat() if published_at is not None else None,
                "row_count": int(row["row_count"] or 0),
            }
        )
    return result


async def _list_preview_periods_postgres(*, job_id: str) -> list[str]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()
    conn = await asyncpg.connect(_pg_dsn())
    try:
        rows = await conn.fetch(
            f"""
            SELECT DISTINCT properties->>'period' AS period
            FROM {table_name}
            WHERE job_id = $1
              AND properties ? 'period'
            ORDER BY period DESC;
            """,
            job_id,
        )
    finally:
        await conn.close()
    return [str(row["period"]) for row in rows if row["period"] is not None]
