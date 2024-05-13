import json
import logging
import time
from decimal import Decimal
from typing import List

import boto3

from precalculator.models import Metadata, Prediction

logger = logging.Logger("DynamoDBWriter")


def write_precalcs_batch_writer(dynamodb_table: str, precalcs: List[Prediction]) -> None:
    dynamodb = boto3.resource("dynamodb")
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


def write_metadata(bucket: str, metadata_key: str, metadata: Metadata) -> None:
    s3 = boto3.client("s3")

    metadata_json = {
        "model_id": metadata.model_id,
        "preds_in_store": metadata.preds_in_store,
        "total_unique_preds": metadata.total_unique_preds,
        "preds_last_updated": metadata.preds_last_updated,
        "pipeline_latest_start_time": metadata.pipeline_latest_start_time,
        "pipeline_latest_duration": metadata.pipeline_latest_duration,
        "pipeline_meta_s3_uri": metadata.pipeline_meta_s3_uri,
    }

    s3.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(metadata_json))
