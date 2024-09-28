import json
import os
import boto3


def handler(event, context):
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
