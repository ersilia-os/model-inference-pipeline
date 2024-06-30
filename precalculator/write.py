import json
import logging
import time
from decimal import Decimal
from typing import List

import boto3

from precalculator.models import Metadata, Prediction

logger = logging.Logger("DynamoDBWriter")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def write_precalcs_batch_writer(dynamodb_table: str, precalcs: List[Prediction]) -> None:
    table = dynamodb.Table(dynamodb_table) # type: ignore

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
    s3_client.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(metadata.json()))
