import json
import os
import boto3


def handler(event, context):
    """Create presigned URL for uploading CSV file to S3

    Args:
        event (dict): _description_
        context (dict): _description_

    Returns:
        response (str): pre-signed url
    """
    model_id = event["queryStringParameters"]["modelid"]
    request_id = event["queryStringParameters"]["requestid"]
    # Generate pre-signed URL for uploading
    s3_client = boto3.client("s3")
    response = s3_client.generate_presigned_post(
        os.environ.get("BUCKET_NAME"),
        f"{os.environ.get('S3_UPLOAD_PREFIX')}/{model_id}/{request_id}.csv",
        ExpiresIn=3600,
    )

    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(response)}
