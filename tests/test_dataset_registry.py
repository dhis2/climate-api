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
  period_type: daily
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define sync_kind"):
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
  period_type: daily
  sync_kind: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync_kind 'sometimes'"):
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
  period_type: daily
  sync_kind: temporal
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["id"] == "valid_temporal"


def test_dataset_registry_accepts_derived_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "valid_derived.yaml"
    registry_file.write_text(
        """
- id: derived_weekly
  name: Derived weekly
  variable: value
  period_type: weekly
  sync_kind: derived
  resample:
    source_dataset_id: source_daily
    method: sum
    week_start: monday
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync_kind"] == "derived"


def test_dataset_registry_rejects_derived_sync_kind_without_resample(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "derived_missing_resample.yaml"
    registry_file.write_text(
        """
- id: derived_missing_resample
  name: Derived missing resample
  variable: value
  period_type: weekly
  sync_kind: derived
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="must define resample when sync_kind is derived"):
        datasets.list_datasets()


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
  period_type: daily
  sync_kind: temporal
  sync_execution: sometimes
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported sync_execution 'sometimes'"):
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
  period_type: daily
  sync_kind: temporal
  sync_execution:
    - append
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync_execution"):
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
  period_type: daily
  sync_kind: temporal
  sync_execution: append
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync_execution"] == "append"


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
  period_type: daily
  sync_kind: temporal
  sync_availability:
    latest_available_function: 42
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="invalid sync_availability.latest_available_function"):
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
  period_type: daily
  sync_kind: temporal
  sync_availability:
    latest_available_function: climate_api.providers.availability.lagged_latest_available
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    assert datasets.list_datasets()[0]["sync_availability"]["latest_available_function"].endswith(
        "lagged_latest_available"
    )


def test_dataset_registry_rejects_invalid_resample_method(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_resample_method.yaml"
    registry_file.write_text(
        """
- id: derived_invalid_method
  name: Derived invalid method
  variable: value
  period_type: weekly
  sync_kind: derived
  resample:
    source_dataset_id: source_daily
    method: median
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="unsupported resample.method 'median'"):
        datasets.list_datasets()


def test_dataset_registry_rejects_resample_for_non_derived_sync_kind(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "temporal_with_resample.yaml"
    registry_file.write_text(
        """
- id: temporal_with_resample
  name: Temporal with resample
  variable: value
  period_type: weekly
  sync_kind: temporal
  resample:
    source_dataset_id: source_daily
    method: sum
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="may only define resample when sync_kind is derived"):
        datasets.list_datasets()


def test_dataset_registry_rejects_week_start_for_non_weekly_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    registry_file = tmp_path / "invalid_resample_week_start.yaml"
    registry_file.write_text(
        """
- id: derived_monthly_with_week_start
  name: Derived monthly with week start
  variable: value
  period_type: monthly
  sync_kind: derived
  resample:
    source_dataset_id: source_daily
    method: sum
    week_start: monday
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(datasets, "CONFIGS_DIR", tmp_path)

    with pytest.raises(ValueError, match="may only define resample.week_start when period_type is weekly"):
        datasets.list_datasets()
