"""Routes for derived processing operations."""

from typing import Any

from fastapi import APIRouter, Body, HTTPException

from climate_api.data_registry.services import processes as process_registry

router = APIRouter()


@router.post("/{process_id}/execution")
def run_process_execution(
    process_id: str,
    request: dict[str, Any] = Body(...),
) -> Any:
    """Dispatch to a registered process execution function by process id."""
    process = process_registry.get_process(process_id)
    if process is None:
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    func = process_registry._get_dynamic_function(process["execution_function"])
    try:
        return func(**request)
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
