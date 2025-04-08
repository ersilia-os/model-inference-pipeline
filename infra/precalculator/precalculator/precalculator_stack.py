from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_athena as athena,
    aws_glue as glue,
    aws_s3 as s3,
    aws_iam as iam,
)

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

        ### IAM ###
        glue_role = iam.Role(
            self,
            id="glue-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:Abort*",
                    "s3:DeleteObject*",
                    "s3:GetBucket*",
                    "s3:GetObject*",
                    "s3:List*",
                    "s3:PutObject",
                    "s3:PutObjectLegalHold",
                    "s3:PutObjectRetention",
                    "s3:PutObjectTagging",
                    "s3:PutObjectVersionTagging",
                ],
                resources=[f"arn:aws:s3:::{BUCKET_NAME}", f"arn:aws:s3:::{BUCKET_NAME}/*"],
                effect=iam.Effect.ALLOW,
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:PutObjectLegalHold",
                    "s3:PutObjectRetention",
                    "s3:PutObjectTagging",
                    "s3:PutObjectVersionTagging",
                ],
                resources=[
                    f"arn:aws:s3:::aws-athena-query-results-{self.region}-{self.account}",
                    f"arn:aws:s3:::aws-athena-query-results-{self.region}-{self.account}/*",
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:BatchGetPartition",
                    "glue:GetColumnStatisticsForPartition",
                    "glue:GetPartition",
                    "glue:GetPartitionIndexes",
                    "glue:GetPartitions",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreateTable",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartitionIndex",
                    "glue:CreatePartition",
                ],
                resources=["*"],
                effect=iam.Effect.ALLOW,
            )
        )

        # allow deletion of temporary resources at a catalog, database, and table level
        # (required for Lambdas to function properly)
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:DeleteTable"],
                resources=[
                    f"arn:aws:glue:{self.region}:{self.account}:{self.account}",
                    f"arn:aws:glue:{self.region}:{self.account}:database/{ATHENA_DATABASE}",
                    f"arn:aws:glue:{self.region}:{self.account}:table/{ATHENA_DATABASE}/temp_table*",
                ],
                effect=iam.Effect.ALLOW,
            )
        )

        # TODO: restrict to only the necessary actions and resources
        glue_role.add_to_policy(iam.PolicyStatement(actions=["athena:*"], resources=["*"], effect=iam.Effect.ALLOW))

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
            role=glue_role,
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
            role=glue_role,
        )

        # TODO: add aws-swk-pandas 3.9.1 as a layer via CDK:
        # https://github.com/aws/aws-sdk-pandas/releases/tag/3.9.1
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
            role=glue_role,
        )

        ### API Gateway ###

        # NOTE: apigateway.LambdaRestApi is unsuitable for cases where
        # the default integration (/) is not used as it requires a handler
        # to be passed to handle all requests which this stack does not have
        # or currently need. Instead, a standard RestApi is used and a Lambda
        # is specified as the integration for each endpoint.
        api = apigateway.RestApi(
            self,
            id=ENDPOINT_NAME,
            description="API Endpoint for Precalculations created via CDK",
            endpoint_configuration=apigateway.EndpointConfiguration(types=[apigateway.EndpointType.REGIONAL]),
            deploy=True,
            # TODO: create a stage for prod
            deploy_options=apigateway.StageOptions(stage_name="dev", description="DEV deployment of Precalcs Endpoint"),
        )

        # GET /precalculations/model
        precalculations = api.root.add_resource("precalculations")
        model = precalculations.add_resource("model")
        model.add_method(
            "GET",
            apigateway.LambdaIntegration(get_model),
            request_parameters={"method.request.querystring.modelid": True},
        )
        # Allow API Gateway to invoke the Lambda
        get_model.add_permission(
            "apigateway",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{api.arn_for_execute_api()}/*/GET/precalculations/model",
        )

        # POST /precalculations/predictions
        predictions = precalculations.add_resource("predictions")
        predictions.add_method(
            "POST",
            apigateway.LambdaIntegration(generate_presigned_url),
            request_parameters={
                "method.request.querystring.modelid": True,
                "method.request.querystring.requestid": True,
                "method.request.querystring.userid": False,
            },
        )
        # Allow API Gateway to invoke the Lambda
        fetch_predictions.add_permission(
            "apigateway",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{api.arn_for_execute_api()}/*/POST/precalculations/predictions",
        )

        # GET /precalculations/upload-destination
        upload_destination = precalculations.add_resource("upload-destination")
        upload_destination.add_method(
            "GET",
            apigateway.LambdaIntegration(fetch_predictions),
            request_parameters={
                "method.request.querystring.modelid": True,
                "method.request.querystring.requestid": True,
            },
        )
        # Allow API Gateway to invoke the Lambda
        generate_presigned_url.add_permission(
            "apigateway",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{api.arn_for_execute_api()}/*/GET/precalculations/upload-destination",
        )

        ### Athena ###
        athena_workgroup = athena.CfnWorkGroup(
            self,
            id="precalculations",
            name="precalculations",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{BUCKET_NAME}/aws-athena-query-results-{self.account}-{self.region}"
                )
            ),
        )

        ### Glue ###
        glue_database = glue.CfnDatabase(
            self,
            id="precalculations",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseIdentifierProperty(database_name=ATHENA_DATABASE),
        )

        predictions_table = glue.CfnTable(
            self,
            catalog_id=self.account,
            id=ATHENA_PREDICTION_TABLE,
            database_name=glue_database.database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=ATHENA_PREDICTION_TABLE,
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "parquet"},
                partition_keys=[glue.CfnTable.ColumnProperty(name="model_id")],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="key", type="string"),
                        glue.CfnTable.ColumnProperty(name="input", type="string"),
                        glue.CfnTable.ColumnProperty(name="output", type="string"),
                        glue.CfnTable.ColumnProperty(name="model_id", type="string"),
                    ],
                    location=f"s3://{BUCKET_NAME}/{ATHENA_PREDICTION_TABLE}",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )

        requests_table = glue.CfnTable(
            self,
            catalog_id=self.account,
            id=ATHENA_REQUEST_TABLE,
            database_name=glue_database.database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name=ATHENA_REQUEST_TABLE,
                table_type="EXTERNAL_TABLE",
                parameters={"classification": "parquet"},
                partition_keys=[glue.CfnTable.ColumnProperty(name="request_id")],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(name="input", type="string"),
                        glue.CfnTable.ColumnProperty(name="request_id", type="string"),
                    ],
                    location=f"s3://{BUCKET_NAME}/{S3_INPUT_PREFIX}",
                    input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters={"serialization.format": "1"},
                    ),
                ),
            ),
        )

        ### S3 ###
        bucket = s3.Bucket(self, id=BUCKET_NAME, bucket_name=BUCKET_NAME, public_read_access=False, versioned=False)
        bucket.grant_read_write(athena_workgroup)
        bucket.grant_read_write(glue_role)
