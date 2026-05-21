"""Internal streaming ingest engine.

The public ingestion API continues to live under `climate_api.ingestions`.
This package contains the internal execution pieces for the new per-period
streaming path introduced for issue #64 / CLIM-715.
"""

from climate_api.streaming.orchestrator import StreamingIngestResult, run_streaming_ingest_sync
from climate_api.streaming.protocol import GridSpec, IngestionPlugin

__all__ = ["GridSpec", "IngestionPlugin", "StreamingIngestResult", "run_streaming_ingest_sync"]
