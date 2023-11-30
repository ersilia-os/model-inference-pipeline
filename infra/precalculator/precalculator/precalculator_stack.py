import os
from dotenv import load_dotenv
from pathlib import Path

from aws_cdk import (
    # Duration,
    Stack,
    RemovalPolicy,
    Duration,
    # aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
)
from constructs import Construct

dotenv_path = Path("../../config/stack.env")
load_dotenv(dotenv_path=dotenv_path)

BUCKET_NAME = str(os.getenv("S3_BUCKET_NAME"))
TABLE_NAME = str(os.getenv("DYNAMODB_TABLE_NAME"))
HANDLER_NAME = str(os.getenv("API_HANDLER_LAMBDA_NAME"))
ENDPOINT_NAME = str(os.getenv("API_ENDPOINT_NAME"))

class PrecalculatorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # S3 Bucket
        bucket = s3.Bucket(
            self, 
            BUCKET_NAME, 
            bucket_name=BUCKET_NAME, 
            public_read_access=False,
            versioned=False,
        )

        # DynamoDB Table
        table = dynamodb.TableV2(
            self, 
            TABLE_NAME, 
            table_name=TABLE_NAME,
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="SK", type=dynamodb.AttributeType.STRING
            ),
            billing=dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.RETAIN, # table is retained upon stack destruction
        )

        # API Lambda
        api_handler = lambda_.Function(
            self, 
            HANDLER_NAME,
            function_name=HANDLER_NAME,
            runtime=lambda_.Runtime.PYTHON_3_10,
            code=lambda_.Code.from_asset("lambda"),
            handler="api_handler.handler",
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "TABLE_NAME": table.table_name
            },
            memory_size=512,
            timeout=Duration.seconds(30),
        )

        # grant permissions for lambda to write to bucket 
        bucket.grant_read_write(api_handler)

        # and read from table
        table.grant_read_data(api_handler)


        # API Gateway
        api_gateway = apigateway.LambdaRestApi(
            self,
            ENDPOINT_NAME,
            handler=api_handler,
        )