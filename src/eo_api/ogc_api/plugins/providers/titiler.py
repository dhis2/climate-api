"""TiTiler Zarr tile provider plugin for pygeoapi."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import requests
from pygeoapi.models.provider.base import TileMatrixSetEnum
from pygeoapi.provider.base import (
    ProviderConnectionError,
    ProviderGenericError,
    ProviderInvalidQueryError,
)
from pygeoapi.provider.tile import BaseTileProvider, ProviderTileNotFoundError
from pygeoapi.util import is_url, url_join

_DEFAULT_TITILER_BASE_URL = "http://127.0.0.1:8000"
_DEFAULT_TITILER_ENDPOINT = "/zarr/tiles"
_DATETIME_TILESET_SEPARATOR = "~dt~"


class TiTilerProvider(BaseTileProvider):
    """Bridge pygeoapi OGC API Tiles requests to TiTiler endpoints."""

    def __init__(self, provider_def: dict[str, Any]) -> None:
        format_name = provider_def["format"]["name"].lower()
        if format_name not in {"png", "jpeg", "jpg", "webp"}:
            raise RuntimeError("TiTiler format must be png, jpeg, jpg, or webp")

        options = dict(provider_def.get("options") or {})
        scheme = options.get("scheme")
        schemes = options.get("schemes")
        if not schemes:
            options["schemes"] = [scheme] if scheme else ["WebMercatorQuad"]

        options.setdefault("endpoint", _DEFAULT_TITILER_ENDPOINT)
        options.setdefault(
            "endpoint_base",
            os.getenv("TITILER_BASE_URL", _DEFAULT_TITILER_BASE_URL),
        )
        options.setdefault("timeout", 30)
        options.setdefault("datetime_param", "datetime")
        options.setdefault("datetime_default", None)

        provider_def = {**provider_def, "options": options}
        super().__init__(provider_def)

        self.tile_type = "raster"
        self._layer = Path(self.data).stem

    def __repr__(self) -> str:
        return f"<TiTilerProvider> {self.data}"

    def get_layer(self) -> None:
        return None

    def get_fields(self) -> dict[str, Any]:
        return {}

    @property
    def endpoint(self) -> str:
        configured = str(self.options.get("endpoint", _DEFAULT_TITILER_ENDPOINT))
        if is_url(configured):
            return configured.rstrip("/")

        base = str(self.options.get("endpoint_base", _DEFAULT_TITILER_BASE_URL)).rstrip("/")
        if configured.startswith("/"):
            return f"{base}{configured}".rstrip("/")
        return f"{base}/{configured}".rstrip("/")

    def get_tiling_schemes(self) -> list[Any]:
        configured = set(self.options.get("schemes", []))
        tile_matrix_set_enum = cast(Any, TileMatrixSetEnum)
        tile_matrix_set_links = [enum.value for enum in tile_matrix_set_enum if enum.value.tileMatrixSet in configured]
        if not tile_matrix_set_links:
            raise ProviderConnectionError("Could not identify any valid tiling scheme")
        return tile_matrix_set_links

    def get_tiles_service(
        self,
        baseurl: str | None = None,
        servicepath: str | None = None,
        dirpath: str | None = None,
        tile_type: str | None = None,
    ) -> dict[str, list[dict[str, str]]]:
        del dirpath, tile_type

        format_name = self.format_type
        if servicepath is None:
            servicepath = (
                "collections/{dataset}/tiles/{tileMatrixSetId}/"
                "{tileMatrix}/{tileRow}/{tileCol}?f="
                f"{format_name}"
            )

        if baseurl and not servicepath.startswith("http"):
            self._service_url = url_join(baseurl, servicepath)
        else:
            self._service_url = servicepath

        query = urlencode(self._titiler_query_params())
        default_scheme = self.options["schemes"][0]
        titiler_href = (
            f"{self.endpoint}/{default_scheme}/{{tileMatrix}}/{{tileCol}}/{{tileRow}}.{format_name}?{query}"
        )

        return {
            "links": [
                {
                    "type": self.mimetype,
                    "rel": "item",
                    "title": "This collection as image tiles",
                    "href": self._service_url,
                },
                {
                    "type": self.mimetype,
                    "rel": "alternate",
                    "title": "Direct TiTiler tile URL template",
                    "href": titiler_href,
                },
            ]
        }

    def get_tiles(
        self,
        layer: str | None = None,
        tileset: str | None = None,
        z: int | None = None,
        y: int | None = None,
        x: int | None = None,
        format_: str | None = None,
    ) -> bytes | None:
        del layer

        if tileset is None:
            raise ProviderInvalidQueryError("Missing tileset identifier")
        if z is None or y is None or x is None:
            raise ProviderInvalidQueryError("Missing tile coordinates")

        normalized_tileset, datetime_ = self._parse_tileset_datetime(tileset)

        try:
            z_i = int(z)
            y_i = int(y)
            x_i = int(x)
        except (TypeError, ValueError) as err:
            raise ProviderInvalidQueryError("Invalid tile coordinates") from err

        tms = self.get_tilematrixset(normalized_tileset)
        if tms is None or not self.is_in_limits(tms, z_i, x_i, y_i):
            raise ProviderTileNotFoundError

        tile_format = (format_ or self.format_type).lower()
        if tile_format == "jpg":
            tile_format = "jpeg"

        request_url = f"{self.endpoint}/{normalized_tileset}/{z_i}/{x_i}/{y_i}.{tile_format}"

        try:
            response = requests.get(
                request_url,
                params=self._titiler_query_params(datetime_),
                timeout=int(self.options.get("timeout", 30)),
            )
        except requests.RequestException as exc:
            raise ProviderConnectionError(str(exc)) from exc

        if response.status_code == 204:
            return None
        if response.status_code == 404:
            raise ProviderTileNotFoundError
        if response.status_code < 500 and not response.ok:
            raise ProviderInvalidQueryError(response.text)
        if response.status_code >= 500:
            raise ProviderGenericError(response.text)

        return cast(bytes, response.content)

    def _titiler_query_params(self, datetime_: str | None = None) -> dict[str, Any]:
        ignored = {
            "scheme",
            "schemes",
            "endpoint",
            "endpoint_base",
            "timeout",
            "datetime_param",
            "datetime_default",
        }
        params = {"url": self.data}
        params.update({k: v for k, v in self.options.items() if k not in ignored})
        effective_datetime = datetime_ if datetime_ is not None else self.options.get("datetime_default")
        if effective_datetime is not None:
            datetime_param = str(self.options.get("datetime_param", "datetime"))
            params[datetime_param] = str(effective_datetime)
        return params

    def _parse_tileset_datetime(self, tileset: str) -> tuple[str, str | None]:
        if _DATETIME_TILESET_SEPARATOR not in tileset:
            return tileset, None

        normalized_tileset, encoded_datetime = tileset.split(_DATETIME_TILESET_SEPARATOR, 1)
        if not normalized_tileset or not encoded_datetime:
            raise ProviderInvalidQueryError("Invalid tileMatrixSetId datetime encoding")

        padding = "=" * (-len(encoded_datetime) % 4)
        try:
            datetime_ = base64.urlsafe_b64decode(f"{encoded_datetime}{padding}").decode("utf-8")
        except Exception as err:
            raise ProviderInvalidQueryError("Invalid tileMatrixSetId datetime encoding") from err

        return normalized_tileset, datetime_

