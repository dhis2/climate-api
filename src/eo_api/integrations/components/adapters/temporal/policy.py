"""Shared temporal capability policy for dataset adapters."""

from __future__ import annotations

from collections.abc import Collection
from typing import Literal, TypedDict

_ALIASES = {"annual": "yearly"}
_ORDER = {
    "hourly": 0,
    "daily": 1,
    "weekly": 2,
    "monthly": 3,
    "yearly": 4,
}


class TemporalDecision(TypedDict):
    """Outcome of evaluating source/requested temporal resolutions."""

    action: Literal["pass_through", "aggregate", "exit"]
    reason: str


def normalize_resolution(value: str) -> str:
    """Normalize temporal resolution with alias handling."""
    normalized = _ALIASES.get(str(value).strip().lower(), str(value).strip().lower())
    if normalized not in _ORDER:
        supported = ", ".join(_ORDER.keys())
        raise ValueError(f"Unsupported temporal resolution '{value}'. Supported: {supported}")
    return normalized


def evaluate_temporal_request(
    *,
    source_resolution: str,
    requested_resolution: str,
    supported_resolutions: Collection[str] | None = None,
    supports_downsample: bool = True,
) -> TemporalDecision:
    """Decide whether temporal stage should pass, aggregate, or exit."""
    source = normalize_resolution(source_resolution)
    requested = normalize_resolution(requested_resolution)

    if supported_resolutions is not None:
        supported = {normalize_resolution(item) for item in supported_resolutions}
        if requested not in supported:
            supported_text = ", ".join(sorted(supported))
            return {
                "action": "exit",
                "reason": f"Requested temporal resolution '{requested}' is not in capability set: {supported_text}.",
            }

    if requested == source:
        return {
            "action": "pass_through",
            "reason": f"Requested temporal resolution '{requested}' matches source '{source}'.",
        }

    if _ORDER[requested] > _ORDER[source]:
        if supports_downsample:
            return {
                "action": "aggregate",
                "reason": f"Aggregating from source '{source}' to coarser '{requested}'.",
            }
        return {
            "action": "exit",
            "reason": f"Aggregation from '{source}' to '{requested}' is not implemented for this dataset.",
        }

    return {
        "action": "exit",
        "reason": (
            f"Requested temporal resolution '{requested}' is finer than source '{source}' and requires disaggregation."
        ),
    }
