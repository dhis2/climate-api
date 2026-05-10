from pathlib import Path

import pytest

from climate_api.data_registry.services import datasets


def test_dataset_registry_requires_sync_kind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    registry_file = tmp_path / "missing_sync_kind.yaml"
    registry_file.write_text(
        """
- id: missing_sync_kind
  name: Missing sync kind
  variable: value
  extents:
    temporal:
      resolution: P1D
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define sync.kind"):
        datasets.list_datasets()


def test_dataset_registry_rejects_unsupported_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_kind.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_kind
  name: Invalid sync kind
  variable: value
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync.kind 'sometimes'"):
        datasets.list_datasets()


def test_dataset_registry_accepts_supported_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid.yaml"
    registry_file.write_text(
        """
- id: valid_temporal
  name: Valid temporal
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: P1D
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["id"] == "valid_temporal"


def test_dataset_registry_rejects_unsupported_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_execution.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_execution
  name: Invalid sync execution
  variable: value
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
    execution: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync.execution 'sometimes'"):
        datasets.list_datasets()


def test_dataset_registry_rejects_non_string_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_execution_type.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_execution_type
  name: Invalid sync execution type
  variable: value
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
    execution:
      - append
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync.execution"):
        datasets.list_datasets()


def test_dataset_registry_accepts_supported_sync_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_append.yaml"
    registry_file.write_text(
        """
- id: valid_append
  name: Valid append
  variable: value
  sync:
    kind: temporal
    execution: append
  extents:
    temporal:
      resolution: P1D
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync"]["execution"] == "append"


def test_dataset_registry_rejects_invalid_sync_availability_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_sync_availability.yaml"
    registry_file.write_text(
        """
- id: invalid_sync_availability
  name: Invalid sync availability
  variable: value
  sync:
    kind: temporal
    availability:
      latest_available_function: 42
  extents:
    temporal:
      resolution: P1D
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync.availability.latest_available_function"):
        datasets.list_datasets()


def test_dataset_registry_accepts_sync_availability_function(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_sync_availability.yaml"
    registry_file.write_text(
        """
- id: valid_sync_availability
  name: Valid sync availability
  variable: value
  sync:
    kind: temporal
    availability:
      latest_available_function: climate_api.providers.availability.lagged_latest_available
  extents:
    temporal:
      resolution: P1D
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync"]["availability"]["latest_available_function"].endswith(
        "lagged_latest_available"
    )


def test_dataset_registry_stores_iso_resolutions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "resolutions.yaml"
    registry_file.write_text(
        """
- id: hourly_ds
  name: Hourly
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: PT1H
  ingestion:
    function: some.download.function
- id: daily_ds
  name: Daily
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: P1D
  ingestion:
    function: some.download.function
- id: weekly_ds
  name: Weekly
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: P1W
  ingestion:
    function: some.download.function
- id: monthly_ds
  name: Monthly
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: P1M
  ingestion:
    function: some.download.function
- id: yearly_ds
  name: Yearly
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: P1Y
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    loaded = {d["id"]: d for d in datasets.list_datasets()}
    assert loaded["hourly_ds"]["extents"]["temporal"]["resolution"] == "PT1H"
    assert loaded["daily_ds"]["extents"]["temporal"]["resolution"] == "P1D"
    assert loaded["weekly_ds"]["extents"]["temporal"]["resolution"] == "P1W"
    assert loaded["monthly_ds"]["extents"]["temporal"]["resolution"] == "P1M"
    assert loaded["yearly_ds"]["extents"]["temporal"]["resolution"] == "P1Y"


def test_dataset_registry_rejects_missing_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "missing_resolution.yaml"
    registry_file.write_text(
        """
- id: missing_resolution
  name: Missing resolution
  variable: value
  sync:
    kind: temporal
  extents:
    temporal: {}
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define extents.temporal.resolution"):
        datasets.list_datasets()


def test_dataset_registry_rejects_missing_extents(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "missing_extents.yaml"
    registry_file.write_text(
        """
- id: missing_extents
  name: Missing extents
  variable: value
  sync:
    kind: temporal
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define extents.temporal.resolution"):
        datasets.list_datasets()


def test_dataset_registry_accepts_non_standard_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "non_standard_resolution.yaml"
    registry_file.write_text(
        """
- id: non_standard_resolution
  name: Non-standard resolution
  variable: value
  sync:
    kind: temporal
  extents:
    temporal:
      resolution: PT6H
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    loaded = datasets.list_datasets()
    assert loaded[0]["extents"]["temporal"]["resolution"] == "PT6H"
