import argparse

import pandas as pd

from config.app import DataLakeConfig
from precalculator.fetcher import PredictionFetcher


def get_parameters() -> dict:
    parser = argparse.ArgumentParser(description="Process parameters.")
    parser.add_argument("--user", help="User ID")
    parser.add_argument("--request", help="Request ID")
    parser.add_argument("--model", help="Model ID")

    args = parser.parse_args()

    params = {"user_id": args.user, "request_id": args.request, "model_id": args.model}

    return params


def main(user_id: str, request_id: str, model_id: str) -> pd.DataFrame:
    """Fetch predictions

    Args:
        user_id (str): _description_
        request_id (str): _description_
        model_id (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    config = DataLakeConfig()  # type: ignore

    print("config", config)

    fetcher = PredictionFetcher(config, user_id, request_id, model_id)

    path_to_input = fetcher.get_s3_input_location()

    print(path_to_input)

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
    return {"event": len(event), "context": len(context)}


if __name__ == "__main__":
    """We are able to call the main function locally as a script"""
    params = get_parameters()

    print("params = ", params)

    df = main(**params)

    df.to_csv("result.csv")
