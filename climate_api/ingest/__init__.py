"""Per-period Icechunk ingest — protocol, orchestrator, and built-in plugins."""

from climate_api.ingest.orchestrator import run_ingest, run_ingest_sync
from climate_api.ingest.protocol import GridSpec, IngestionPlugin

__all__ = ["GridSpec", "IngestionPlugin", "run_ingest", "run_ingest_sync"]
