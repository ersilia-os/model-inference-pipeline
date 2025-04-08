import pandas as pd
from pydantic import BaseModel, Field


class Prediction(BaseModel):
    """Dataclass to represent a single prediction"""

    key: str
    input: str
    output: dict
    model_id: str


class Metadata(BaseModel):
    """Dataclass to represent metadata for a pipeline run"""

    model_id: str = Field(default="")
    preds_in_store: bool = Field(default=False)
    total_unique_preds: int = Field(default=0)
    preds_last_updated: int = Field(default=0)
    pipeline_latest_start_time: int = Field(default=0)
    pipeline_latest_duration: int = Field(default=0)
    pipeline_meta_s3_uri: str = Field(default="")


class SchemaValidationError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        error_message = "\n".join(errors)
        super().__init__(f"Schema validation failed with the following errors:\n{error_message}\n")


def validate_dataframe_schema(df: pd.DataFrame, model: BaseModel) -> None:
    errors = []
    schema = model.model_fields

    for field_name, field in schema.items():
        if field_name not in df.columns:
            errors.append(f"Missing column: {field_name}")
        else:
            pandas_dtype = df[field_name].dtype
            pydantic_type = field.annotation
            if not _check_type_compatibility(pandas_dtype, pydantic_type):
                errors.append(f"Column {field_name} has type {pandas_dtype}, expected {pydantic_type}")

    for column in df.columns:
        if column not in schema:
            errors.append(f"Unexpected column: {column}")

    if errors:
        raise SchemaValidationError(errors)


def _check_type_compatibility(pandas_dtype, pydantic_type) -> bool:  # noqa: ANN001
    type_map = {
        "object": [str, dict, list],
        "int64": [int],
        "float64": [float],
        "bool": [bool],
        "datetime64": [pd.Timestamp],
    }
    return pydantic_type in type_map.get(str(pandas_dtype))  # type: ignore
