import os
import boto3


def handler(event: dict, context: dict) -> bool:
    """Check model exists

    Args:
        event (dict): _description_
        context (dict): _description_

    Returns:
        has_model (bool): whether or not precalculations for the model exist
    """
    model_id = event["queryStringParameters"]["modelid"]
    glue_client = boto3.client("glue")
    has_model = False

    try:
        response = glue_client.get_partition(
            DatabaseName=os.environ.get("ATHENA_DATABASE"),
            TableName=os.environ.get("ATHENA_PREDICTION_TABLE"),
            PartitionValues=[model_id],
        )
        has_model = len(response["Partition"]["Values"]) > 0
    except glue_client.exceptions.EntityNotFoundException:
        print(f"Model {model_id} not found")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": has_model}
