import json

import boto3

from config.config import DataLakeConfig
from precalculator.models import Metadata

s3_client = boto3.client("s3")

class PredictionWriter:
    def __init__(self, config: DataLakeConfig, model_id: str):
        self.config = config
        self.model_id = model_id

    def write_metadata(self, bucket: str, metadata_key: str, metadata: Metadata) -> None:
        s3_client.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(metadata.model_dump_json()))
