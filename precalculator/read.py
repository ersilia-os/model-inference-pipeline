import json

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from precalculator.models import Metadata, Prediction

s3_client = boto3.client("s3")
dynamodb_client = boto3.client("dynamodb")


def get_predictions_from_dataframe(model_id: str, prediction_df: pd.DataFrame) -> list[Prediction]:
    """Converts a valid pandas dataframe of predictions into a list of prediction objects

    Args:
        model_id (str): ID of the model which generated the predictions
        prediction_df (DataFrame): pandas dataframe containing predictions

    Returns:
        list[Prediction]: list of prediction objects
    """
    # the first 2 columns should be validated against the base schema
    prediction_keys = prediction_df.iloc[:, :2]

    # the 3rd column onwards should be combined into a single column
    prediction_output = pd.Series(prediction_df.iloc[:, 2:].values.tolist(), name="output")

    # combine into one df and continue
    prediction_df_validated = pd.concat([prediction_keys, prediction_output], axis=1)

    predictions = prediction_df_validated.to_dict("records")

    return [
        Prediction(
            model_id=model_id,
            input_key=prediction["key"],
            smiles=prediction["input"],
            output=prediction["output"],
        )
        for prediction in predictions
    ]


def get_metadata(
    bucket: str, table_name: str, metadata_key: str, s3_uri: str, model_id: str, start_time: int, end_time: int
) -> Metadata:
    try:
        metadata_obj = s3_client.get_object(Bucket=bucket, Key=metadata_key)
        metadata_string = metadata_obj["Body"].read().decode("utf-8")
        metadata_json = json.loads(metadata_string)
        metadata = Metadata(**metadata_json)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            metadata = Metadata()

    metadata.model_id = model_id
    metadata.pipeline_latest_start_time = start_time
    metadata.preds_last_updated = end_time
    metadata.pipeline_latest_duration = metadata.preds_last_updated - metadata.pipeline_latest_start_time
    metadata.pipeline_meta_s3_uri = s3_uri

    model_id_search = dynamodb_client.scan(
        TableName=table_name,
        FilterExpression="SK=:model_id",
        ExpressionAttributeValues={":model_id": {"S": f"MODELID#{model_id}"}},
        Select="COUNT",
    )
    if model_id_search["Count"] > 0:
        metadata.preds_in_store = True
        metadata.total_unique_preds = model_id_search["Count"]

    return metadata
