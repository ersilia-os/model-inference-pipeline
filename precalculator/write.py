import json
import logging
import time
from decimal import Decimal
from typing import List

import boto3

from precalculator.models import Prediction

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
