"""TiTiler provider"""

from typing import Any
from pygeoapi.provider.tile import BaseTileProvider
from pygeoapi.models.provider.base import TileMatrixSetEnum

class TiTilerProvider(BaseTileProvider):
    """Minimal provider implementation used by pygeoapi plugin loading."""

    def __init__(self, provider_def: dict[str, Any]) -> None:
        options = dict(provider_def.get("options") or {})
        if not options.get("schemes"):
            scheme = options.get("scheme")
            options["schemes"] = [scheme] if scheme else ["WebMercatorQuad"]

        zoom = dict(options.get("zoom") or {})
        zoom.setdefault("min", 0)
        zoom.setdefault("max", 24)
        options["zoom"] = zoom

        provider_def = {**provider_def, "options": options}

        super().__init__(provider_def)
        
        self.tile_type = 'raster'

    def __repr__(self) -> str:
        return f"<TiTilerProvider> {self.data}"
    
    def get_layer(self):
        raise NotImplementedError()

    def get_tiling_schemes(self) -> list[Any]:
        return [
            TileMatrixSetEnum.WEBMERCATORQUAD.value # type: ignore
        ]

    def get_tiles_service(
        self,
        baseurl: str | None = None,
        servicepath: str | None = None,
        dirpath: str | None = None,
        tile_type: str | None = None,
    ) -> dict[str, list[dict[str, str]]]:
       
        return {
            "links": []
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
        return None
    