"""Instance configuration loaded from CLIMATE_API_CONFIG."""

import os
import re
from pathlib import Path
from typing import Any

import yaml


def _substitute_env_vars(text: str) -> str:
    """Replace ${VAR:-default} patterns with values from the environment."""

    def _replace(match: re.Match[str]) -> str:
        var, _, default = match.group(1).partition(":-")
        return os.environ.get(var, default)

    return re.sub(r"\$\{([^}]+)\}", _replace, text)


def get_config_path() -> Path | None:
    """Return the resolved Path of CLIMATE_API_CONFIG, or None if unset."""
    raw = os.environ.get("CLIMATE_API_CONFIG")
    return Path(raw).resolve() if raw else None


def get_config() -> dict[str, Any]:
    """Load and return the instance config from CLIMATE_API_CONFIG.

    Results are cached for the lifetime of the process; the config file is
    read once and reused on subsequent calls. Returns an empty dict if
    CLIMATE_API_CONFIG is not set. Raises FileNotFoundError if the path is
    set but does not exist.
    """
    return _load_config()


# Module-level cache — reset between tests via monkeypatch on _cache.
_cache: dict[str, Any] | None = None


def _load_config() -> dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache
    path = get_config_path()
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"CLIMATE_API_CONFIG not found: {path}")
    text = _substitute_env_vars(path.read_text(encoding="utf-8"))
    loaded = yaml.safe_load(text)
    if loaded is not None and not isinstance(loaded, dict):
        raise ValueError(f"CLIMATE_API_CONFIG must be a YAML mapping at the top level: {path}")
    _cache = dict(loaded or {})
    return _cache
