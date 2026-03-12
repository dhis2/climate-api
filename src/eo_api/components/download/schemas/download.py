from pydantic import BaseModel


class _DownloadDatasetStepConfig(BaseModel):
    # from workflows folder
    model_config = ConfigDict(extra="forbid")

    overwrite: bool | None = None
    country_code: str | None = None
