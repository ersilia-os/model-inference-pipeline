from pydantic_settings import BaseSettings, SettingsConfigDict


class DataLakeConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file="config/settings.env", env_file_encoding="utf-8", extra="ignore")

    s3_bucket_name: str
    s3_input_prefix: str
    s3_output_prefix: str
    s3_upload_prefix: str

    athena_database: str
    athena_prediction_table: str
    athena_request_table: str
