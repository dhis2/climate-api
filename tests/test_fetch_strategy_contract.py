from pathlib import Path
from typing import Any

from eo_api.integrations.components.services.chirps3_fetch_service import resolve_chirps3_files
from eo_api.integrations.components.services.era5_land_fetch_service import resolve_era5_land_files


def test_resolve_chirps3_files_rejects_unsupported_output_format(tmp_path: Path) -> None:
    result = resolve_chirps3_files(
        start="2025-01",
        end="2025-01",
        bbox=[-1.0, -1.0, 1.0, 1.0],
        stage="final",
        flavor="rnl",
        download_root=tmp_path,
        output_format="zarr",
    )
    assert result["files"] == []
    assert result["implementation_status"] == "not_implemented"
    assert "not implemented" in str(result["not_implemented_reason"])


def test_resolve_era5_files_rejects_unsupported_output_format(tmp_path: Path) -> None:
    result = resolve_era5_land_files(
        start="2025-01-01",
        end="2025-01-02",
        bbox=[-1.0, -1.0, 1.0, 1.0],
        variables=["2m_temperature"],
        download_root=tmp_path,
        output_format="zarr",
    )
    assert result["files"] == []
    assert result["implementation_status"] == "not_implemented"
    assert "not implemented" in str(result["not_implemented_reason"])


def test_resolve_era5_files_returns_existing_downloads(monkeypatch: Any, tmp_path: Path) -> None:
    expected = tmp_path / "era5_202501.nc"
    expected.write_text("stub", encoding="utf-8")

    def _fake_download(**_: object) -> list[str]:
        return [str(expected)]

    from eo_api.integrations.components.services import era5_land_fetch_service

    monkeypatch.setattr(era5_land_fetch_service.era5_land_hourly, "download", _fake_download)
    result = resolve_era5_land_files(
        start="2025-01-01",
        end="2025-01-02",
        bbox=[-1.0, -1.0, 1.0, 1.0],
        variables=["2m_temperature"],
        download_root=tmp_path,
        output_format="netcdf",
    )
    assert result["files"] == [str(expected)]
    assert result["implementation_status"] == "ok"
