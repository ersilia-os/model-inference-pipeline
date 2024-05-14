from typing import Any

import pandera as pa
from pydantic import BaseModel, Field


class Prediction(BaseModel):
    """Dataclass to represent a single prediction"""

    model_id: str
    input_key: str
    smiles: str
    output: list[Any]


class BasePredictionSchema(pa.DataFrameModel):
    key: str = pa.Field(str_matches=(r"^[a-zA-Z]{14}-[a-zA-Z]{10}-[a-zA-Z]{1}$"))
    input: str


class Metadata(BaseModel):
    """Dataclass to represent metadata for a pipeline run"""

    model_id: str = Field(default="")
    preds_in_store: bool = Field(default=False)
    total_unique_preds: int = Field(default=0)
    preds_last_updated: int = Field(default=0)
    pipeline_latest_start_time: int = Field(default=0)
    pipeline_latest_duration: int = Field(default=0)
    pipeline_meta_s3_uri: str = Field(default="")
