"""Command-line entry point for running the Climate API with uvicorn."""

import uvicorn


def main() -> None:
    """Start the Climate API server."""
    uvicorn.run("climate_api.main:app", host="0.0.0.0", port=8000)
