import json

import boto3

from precalculator.models import Metadata

s3_client = boto3.client("s3")


def write_metadata(bucket: str, metadata_key: str, metadata: Metadata) -> None:
    s3_client.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(metadata.json()))
