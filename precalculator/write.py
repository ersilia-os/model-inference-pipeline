import json
import logging
import time
from decimal import Decimal
from typing import List

import boto3
from botocore.exceptions import ClientError

from precalculator.models import Metadata, Prediction
from precalculator.read import get_metadata

logger = logging.Logger("DynamoDBWriter")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def write_precalcs_batch_writer(dynamodb_table: str, precalcs: List[Prediction]) -> None:
    table = dynamodb.Table(dynamodb_table)

    logger.info(f"Writing {len(precalcs)} using the boto3 batch writer to DynamoDB")

    with table.batch_writer() as writer:
        for item in precalcs:
            writer.put_item(
                Item={
                    "PK": f"INPUTKEY#{item.input_key}",
                    "SK": f"MODELID#{item.model_id}",
                    "Smiles": item.smiles,
                    "Precalculation": json.loads(json.dumps(item.output), parse_float=Decimal),
                    "Timestamp": str(time.time()),
                }
            )


def _write_metadata(bucket: str, key: str, obj: Metadata) -> None:
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj.json()))


def write_metadata_end(
    table_name: str, bucket: str, model_id: str, end_time: int, metadata_key: str, s3_uri: str
) -> None:
    metadata = get_metadata(bucket, metadata_key)

    model_id_search = dynamodb.scan(
        TableName=table_name,
        FilterExpression="SK=:model_id",
        ExpressionAttributeValues={":model_id": {"S": f"MODELID#{model_id}"}},
        Select="COUNT",
    )
    if model_id_search["Count"] > 0:
        metadata.preds_in_store = True
        metadata.total_unique_preds = model_id_search["Count"]

    metadata.preds_last_updated = end_time

    # this assumes that the pipeline start time is the current run's and has been written to metadata already...
    metadata.pipeline_latest_duration = metadata.preds_last_updated - metadata.pipeline_latest_start_time

    metadata.pipeline_meta_s3_uri = s3_uri

    _write_metadata(bucket, metadata_key, metadata)


def write_metadata_start(bucket: str, start_time: int, metadata_key: str) -> None:
    try:
        metadata = get_metadata(bucket, metadata_key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            metadata = Metadata()

    metadata.pipeline_latest_start_time = start_time

    _write_metadata(bucket, metadata_key, metadata)
