from typing import Any

import pytest
from fastapi import HTTPException

from eo_api.data_manager.services import downloader


def test_download_dataset_returns_400_when_country_code_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.delenv("COUNTRY_CODE", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "requires a country code" in str(exc_info.value.detail)


def test_download_dataset_returns_400_for_missing_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        bbox: list[float],
    ) -> None:
        del start, end, dirname, prefix, overwrite, bbox

    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.delenv("DOWNLOAD_BBOX", raising=False)
    monkeypatch.delenv("DEFAULT_DOWNLOAD_BBOX", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)
    monkeypatch.setattr(downloader, "_get_default_bbox", _raise_default_bbox_error)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "A bbox is required" in str(exc_info.value.detail)


def test_download_dataset_returns_502_for_upstream_provider_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code
        raise RuntimeError("provider timeout")

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "cache_info": {"eo_function": "ignored.path"},
    }
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code="SLE",
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 502
    assert "Upstream dataset download failed: provider timeout" == str(exc_info.value.detail)


def _raise_default_bbox_error() -> list[float]:
    raise RuntimeError("missing default bbox")
