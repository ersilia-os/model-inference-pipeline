import pandas as pd

from ersilia_precalc_poc.models import BasePredictionSchema, Prediction

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
