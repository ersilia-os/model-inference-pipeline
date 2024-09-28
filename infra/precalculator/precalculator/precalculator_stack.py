from aws_cdk import Duration, RemovalPolicy, Stack, aws_apigateway as apigateway, aws_lambda as lambda_, aws_s3 as s3
from constructs import Construct

import os
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path("../../config/settings.env")
load_dotenv(dotenv_path=dotenv_path)

BUCKET_NAME = str(os.getenv("S3_BUCKET_NAME"))
S3_UPLOAD_PREFIX = str(os.getenv("S3_UPLOAD_PREFIX"))
S3_INPUT_PREFIX = str(os.getenv("S3_INPUT_PREFIX"))
S3_OUTPUT_PREFIX = str(os.getenv("S3_OUTPUT_PREFIX"))
HANDLER_NAME = str(os.getenv("API_HANDLER_LAMBDA_NAME"))
ENDPOINT_NAME = str(os.getenv("API_ENDPOINT_NAME"))
ATHENA_DATABASE = str(os.getenv("ATHENA_DATABASE"))
ATHENA_PREDICTION_TABLE = str(os.getenv("ATHENA_PREDICTION_TABLE"))
ATHENA_REQUEST_TABLE = str(os.getenv("ATHENA_REQUEST_TABLE"))


class PrecalculatorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ### S3 ###
        bucket = s3.Bucket(self, id=BUCKET_NAME, bucket_name=BUCKET_NAME, public_read_access=False, versioned=False)

        ### Lambda ###
        get_model = lambda_.Function(
            self,
            id="get_model",
            function_name="get_model",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("lambda"),
            handler="get_model.handler",
            environment={"ATHENA_DATABASE": ATHENA_DATABASE, "ATHENA_PREDICTION_TABLE": ATHENA_PREDICTION_TABLE},
            memory_size=512,
            timeout=Duration.seconds(30),
        )

        generate_presigned_url = lambda_.Function(
            self,
            id="generate_presigned_url",
            function_name="generate_presigned_url",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("lambda"),
            handler="generate_presigned_url.handler",
            environment={"BUCKET_NAME": BUCKET_NAME, "S3_UPLOAD_PREFIX": S3_UPLOAD_PREFIX},
            memory_size=512,
            timeout=Duration.seconds(30),
        )

        # TODO: add aws-swk-pandas 3.9.1 as a layer via CDK: https://github.com/aws/aws-sdk-pandas/releases
        fetch_predictions = lambda_.Function(
            self,
            id="fetch_predictions",
            function_name="fetch_predictions",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("lambda"),
            handler="fetch_predictions.handler",
            environment={
                "BUCKET_NAME": BUCKET_NAME,
                "S3_UPLOAD_PREFIX": S3_UPLOAD_PREFIX,
                "S3_INPUT_PREFIX": S3_INPUT_PREFIX,
                "S3_OUTPUT_PREFIX": S3_OUTPUT_PREFIX,
                "ATHENA_DATABASE": ATHENA_DATABASE,
                "ATHENA_REQUEST_TABLE": ATHENA_REQUEST_TABLE,
            },
            memory_size=512,
            timeout=Duration.seconds(120),
        )

        ### API Gateway ###
        api = apigateway.LambdaRestApi(
            self,
            id=ENDPOINT_NAME,
            # handler=api_handler
            description="API Endpoint for Precalculations created via CDK",
            endpoint_configuration=apigateway.EndpointConfiguration(types=[apigateway.EndpointType.REGIONAL]),
            deploy=True,
            deploy_options=apigateway.StageOptions(stage_name="dev", description="DEV deployment of Precalcs Endpoint"),
            proxy=False,
        )
        precalculations = api.root.add_resource("precalculations")
        model = precalculations.add_resource("model")
        # GET /precalculations/model
        model.add_method(
            "GET",
            apigateway.LambdaIntegration(get_model),
            request_parameters={"method.request.querystring.modelid": True},
        )
        predictions = precalculations.add_resource("predictions")
        # POST /precalculations/predictions
        predictions.add_method(
            "POST",
            apigateway.LambdaIntegration(generate_presigned_url),
            request_parameters={
                "method.request.querystring.modelid": True,
                "method.request.querystring.requestid": True,
                "method.request.querystring.userid": False,
            },
        )
        upload_destination = precalculations.add_resource("upload-destination")
        # GET /precalculations/upload-destination
        upload_destination.add_method(
            "GET",
            apigateway.LambdaIntegration(fetch_predictions),
            request_parameters={
                "method.request.querystring.modelid": True,
                "method.request.querystring.requestid": True,
            },
        )
