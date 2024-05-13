import boto3
import pandas as pd

from precalculator.models import BasePredictionSchema, Metadata, Prediction

# def read_predictions_from_s3(model_id: str, s3_config:) -> DataFrame[PredictionSchema]


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
    BasePredictionSchema.validate(prediction_keys)

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


def get_metadata(table_name: str, model_id: str, timestamps: str, s3_uri: str) -> Metadata:
    """Generate a metadata object for a given model ID and pipeline run

    Args:
        model_id (str): ID of the model
        timestamps (str): Start and end times of the pipeline run

    Returns:
        Metadata: Metadata object
    """
    client = boto3.client("dynamodb")

    preds_in_store = False
    total_unique_preds = 0
    model_id_search = client.scan(
        TableName=table_name,
        FilterExpression="SK=:model_id",
        ExpressionAttributeValues={":model_id": {"S": f"MODELID#{model_id}"}},
        Select="COUNT",
    )
    if model_id_search["Count"] > 0:
        preds_in_store = True
        total_unique_preds = model_id_search["Count"]

    start_time, end_time = map(int, timestamps.split())
    duration = end_time - start_time

    return Metadata(
        model_id=model_id,
        preds_in_store=preds_in_store,
        total_unique_preds=total_unique_preds,
        preds_last_updated=end_time,
        pipeline_latest_start_time=start_time,
        pipeline_latest_duration=duration,
        pipeline_meta_s3_uri=s3_uri,
    )
