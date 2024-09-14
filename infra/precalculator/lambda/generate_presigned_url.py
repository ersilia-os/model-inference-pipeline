import json
import boto3
from config.app import DataLakeConfig


def lambda_handler(event, context):
    model_id = event["queryStringParameters"]["modelid"]
    request_id = event["queryStringParameters"]["requestid"]
    # Generate pre-signed URL for uploading
    s3_client = boto3.client("s3")
    response = s3_client.generate_presigned_post(
        "precalculations-bucket", f"uploads/{model_id}/{request_id}.csv", ExpiresIn=3600
    )

    return {"statusCode": 200, "body": json.dumps(response)}
