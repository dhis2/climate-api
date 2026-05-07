"""Server-side HTML rendering for the Climate API management interface."""

import importlib.resources
import logging
from typing import Any

import jinja2
from fastapi import Request

from climate_api.extents.services import get_extent
from climate_api.ingestions.services import list_datasets

_env = jinja2.Environment(loader=jinja2.BaseLoader(), autoescape=True)

_cache: dict[str, jinja2.Template] = {}

_log = logging.getLogger(__name__)


def get_template(name: str) -> jinja2.Template:
    """Load and cache a Jinja2 template from the bundled templates/ directory."""
    if name not in _cache:
        resource = importlib.resources.files("climate_api") / "templates" / name
        _cache[name] = _env.from_string(resource.read_text(encoding="utf-8"))
    return _cache[name]


def _media_type_q(accept: str, media_type: str) -> float:
    """Return the q-value for media_type in an Accept header, or -1.0 if absent."""
    for item in accept.split(","):
        parts = item.strip().split(";")
        if parts[0].strip() != media_type:
            continue
        q = 1.0
        for param in parts[1:]:
            param = param.strip()
            if param.startswith("q="):
                try:
                    q = float(param[2:])
                except ValueError:
                    pass
        return q
    return -1.0


def wants_json(request: Request) -> bool:
    """Return True if the client prefers a JSON response over HTML.

    JSON is preferred when application/json is present with a q-value greater
    than or equal to text/html. HTML wins only when text/html has a strictly
    higher q-value, matching RFC 7231 content negotiation semantics.
    """
    if request.query_params.get("f") == "json":
        return True
    accept = request.headers.get("accept", "")
    if not accept:
        return False
    json_q = _media_type_q(accept, "application/json")
    html_q = _media_type_q(accept, "text/html")
    return json_q >= 0 and (html_q < 0 or json_q >= html_q)


def render_landing(version: str, base: str) -> str:
    """Render the root landing page with live instance status."""
    try:
        extent: dict[str, Any] | None = get_extent()
    except ValueError:
        extent = None
    except Exception:
        _log.exception("Unexpected error loading extent for landing page")
        extent = None
    try:
        datasets = list_datasets().items
    except Exception:
        _log.exception("Unexpected error loading datasets for landing page")
        datasets = []
    return get_template("landing_page.html").render(
        version=version,
        base=base,
        extent=extent,
        datasets=datasets,
    )
