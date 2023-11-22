from typing import Any

import pandera as pa
from pydantic import BaseModel


class Prediction(BaseModel):
    """Dataclass to represent a single prediction"""

    model_id: str
    input_key: str
    smiles: str
    output: list[Any]


class BasePredictionSchema(pa.DataFrameModel):
    key: str = pa.Field(str_matches=(r"^[a-zA-Z]{14}-[a-zA-Z]{10}-[a-zA-Z]{1}$"))
    input: str
