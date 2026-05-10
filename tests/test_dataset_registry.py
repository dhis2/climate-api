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
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
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
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
    execution: append
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
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
    availability:
      latest_available_function: 42
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
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
    availability:
      latest_available_function: climate_api.providers.availability.lagged_latest_available
  ingestion:
    function: some.download.function
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync"]["availability"]["latest_available_function"].endswith(
        "lagged_latest_available"
    )


def test_dataset_registry_derives_period_type_from_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "datasets.yaml"
    registry_file.write_text(
        """
- id: hourly_ds
  name: Hourly
  variable: v
  extents:
    temporal:
      resolution: PT1H
  sync:
    kind: temporal
  ingestion:
    function: some.func

- id: daily_ds
  name: Daily
  variable: v
  extents:
    temporal:
      resolution: P1D
  sync:
    kind: temporal
  ingestion:
    function: some.func

- id: weekly_ds
  name: Weekly
  variable: v
  extents:
    temporal:
      resolution: P1W
  sync:
    kind: temporal
  ingestion:
    function: some.func

- id: monthly_ds
  name: Monthly
  variable: v
  extents:
    temporal:
      resolution: P1M
  sync:
    kind: temporal
  ingestion:
    function: some.func

- id: yearly_ds
  name: Yearly
  variable: v
  extents:
    temporal:
      resolution: P1Y
  sync:
    kind: release
  ingestion:
    function: some.func
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    loaded = {d["id"]: d for d in datasets.list_datasets()}
    assert loaded["hourly_ds"]["period_type"] == "hourly"
    assert loaded["daily_ds"]["period_type"] == "daily"
    assert loaded["weekly_ds"]["period_type"] == "weekly"
    assert loaded["monthly_ds"]["period_type"] == "monthly"
    assert loaded["yearly_ds"]["period_type"] == "yearly"


def test_dataset_registry_rejects_missing_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "no_resolution.yaml"
    registry_file.write_text(
        """
- id: no_resolution
  name: No resolution
  variable: v
  extents:
    temporal:
      begin: "2020-01-01"
  sync:
    kind: temporal
  ingestion:
    function: some.func
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
    registry_file = tmp_path / "no_extents.yaml"
    registry_file.write_text(
        """
- id: no_extents
  name: No extents
  variable: v
  sync:
    kind: temporal
  ingestion:
    function: some.func
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define extents.temporal.resolution"):
        datasets.list_datasets()


def test_dataset_registry_rejects_unrecognised_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "bad_resolution.yaml"
    registry_file.write_text(
        """
- id: bad_resolution
  name: Bad resolution
  variable: v
  extents:
    temporal:
      resolution: P6H
  sync:
    kind: temporal
  ingestion:
    function: some.func
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unrecognised.*extents.temporal.resolution.*P6H"):
        datasets.list_datasets()
