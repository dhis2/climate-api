from pydantic import BaseModel


class DownloadDatasetRunRequest(BaseModel):
    """Execute dataset download component."""

    dataset_id: str
    start: str
    end: str
    overwrite: bool = False
    country_code: str | None = None
    bbox: list[float] | None = None


class DownloadDatasetRunResponse(BaseModel):
    """Download component result."""

    status: str
    dataset_id: str
    start: str
    end: str
