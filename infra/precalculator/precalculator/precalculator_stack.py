from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    aws_s3 as s3,
    aws_lambda,
)
from constructs import Construct

class PrecalculatorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "PrecalculatorQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )
        
        # S3 Bucket
        bucket = s3.Bucket(self, "precalculations")