import json
import logging
import os

import boto3


def handler(event, context):
    # init dynamodb table
    logger = logging.getLogger(__name__)
    dynamodb = boto3.resource("dynamodb")

    table_name = os.environ.get("TABLE_NAME")
    table = dynamodb.Table(table_name)

    if event["body"]:
        item = json.loads(event["body"])
        logging.info(f"## Received payload: {item}")

        # extract params from event
        model_id = item["modelId"]
        input_keys = item["inputKeyArray"]

        # define keys to be searched for batch query
        batch_keys = {
            table.name: {
                "Keys": [{"PK": f"INPUTKEY#{key}", "SK": f"MODELID#{model_id}"} for key in input_keys],
            },
        }

        try:
            dynamo_response = dynamodb.batch_get_item(RequestItems=batch_keys)
        except Exception:
            return {"statusCode": 500}  # catch-all internal server error for any exceptions

        for response_table, response_items in dynamo_response.items():
            logger.info("Got %s items from %s.", len(response_items), response_table)

        # parse smiles and precalc value from response
        results = {
            "model_id": model_id,
            "output": [
                {"smiles": item["Smiles"].replace("\n", ""), "value": item["Precalculation"]}
                for item in dynamo_response["Responses"][table.name]
            ],
        }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": results,
        }
    else:
        logging.info("## Received request without a payload")

        return {"statusCode": 204, "body": {}}  # no content
