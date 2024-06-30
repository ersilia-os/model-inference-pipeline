from pydantic_settings import BaseSettings, SettingsConfigDict


class DataLakeConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file="config/settings.env", env_file_encoding="utf-8")

    s3_bucket: str
    s3_prefix: str

    athena_database: str
    athena_prediction_table: str
    athena_request_table: str
