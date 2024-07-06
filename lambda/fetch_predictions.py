import click
import pandas as pd

from precalculator.fetcher import PredictionFetcher
from config.config import DataLakeConfig


@click.command()
@click.option("--user")
@click.option("--request")
@click.option("--model")
def get_parameters(user: str, request: str, model: str) -> dict[str, str]:
    return {
        "user_id": user,
        "request_id": request,
        "model_id": model,
    }


def main(user_id: str, request_id: str, model_id: str) -> pd.DataFrame:
    """Fetch predictions

    Args:
        user_id (str): _description_
        request_id (str): _description_
        model_id (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    config = DataLakeConfig() # type: ignore

    fetcher = PredictionFetcher(config, user_id, request_id, model_id)

    path_to_input = fetcher.get_s3_input_location()

    df_predictions = fetcher.fetch(path_to_input)

    return df_predictions


def handler(event: dict, context: dict) -> dict:
    """Placeholder lambda function which will call main

    Args:
        event (dict): _description_
        context (dict): _description_

    Returns:
        dict: _description_
    """
    return {
        "event": len(event),
        "context": len(context)
    }


if __name__ == "__main__":
    """We are able to call the main function locally as a script"""
    params = get_parameters()

    df = main(**params)

    df.to_csv("result.csv")
