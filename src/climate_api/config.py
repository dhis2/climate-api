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


def get_config() -> dict[str, Any]:
    """Load and return the instance config from CLIMATE_API_CONFIG.

    Returns an empty dict if CLIMATE_API_CONFIG is not set.
    Raises FileNotFoundError if the path is set but does not exist.
    """
    config_path = os.environ.get("CLIMATE_API_CONFIG")
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"CLIMATE_API_CONFIG not found: {path}")
    text = _substitute_env_vars(path.read_text(encoding="utf-8"))
    return yaml.safe_load(text) or {}
