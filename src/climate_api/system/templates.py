"""Server-side HTML rendering for the Climate API management interface."""

import importlib.resources
from typing import Any

import jinja2
from fastapi import Request

from climate_api.extents.services import get_extent
from climate_api.ingestions.services import list_datasets

_env = jinja2.Environment(loader=jinja2.BaseLoader(), autoescape=True)

_cache: dict[str, jinja2.Template] = {}


def get_template(name: str) -> jinja2.Template:
    """Load and cache a Jinja2 template from the bundled data/templates/ directory."""
    if name not in _cache:
        resource = importlib.resources.files("climate_api") / "data" / "templates" / name
        _cache[name] = _env.from_string(resource.read_text(encoding="utf-8"))
    return _cache[name]


def wants_json(request: Request) -> bool:
    """Return True if the client prefers a JSON response over HTML."""
    if request.query_params.get("f") == "json":
        return True
    accept = request.headers.get("accept", "")
    return "application/json" in accept and "text/html" not in accept


def render_landing(version: str) -> str:
    """Render the root landing page with live instance status."""
    try:
        extent: dict[str, Any] | None = get_extent()
    except Exception:
        extent = None
    try:
        datasets = list_datasets().items
    except Exception:
        datasets = []
    return get_template("index.html").render(
        version=version,
        extent=extent,
        datasets=datasets,
    )
