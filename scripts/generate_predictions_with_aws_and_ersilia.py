import logging
import sys
import time
from typing import List
import os
import subprocess
from ersilia import ErsiliaModel  # type: ignore
import boto3
import pandas as pd

EXAMPLE_MODEL_ID = "eos2zmb"

logger = logging.Logger("logger")


def read_input_from_s3(bucket_name: str, filename: str, local_filename: str) -> None:
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, filename, local_filename)
    logger.info(f"Downloaded {filename} from S3")


def read_input_from_file(local_filename: str = "reference_library.csv") -> List[str]:
    start = time.time()
    with open(local_filename, "r") as file:
        contents = file.readlines()
    logger.info(f"Reading took {time.time() - start :.2f} seconds")
    logger.info(f"Input file has {len(contents)} rows")
    return contents


def generate_predictions(input_path: str, output_path: str, model_id: str) -> pd.DataFrame:
    input_items = read_input_from_file(input_path)

    with ErsiliaModel(model_id) as mdl:
        logger.info(f"Fetched model {model_id}")

        start = time.time()
        predictions = mdl.run(input_items, output="pandas")
        logger.info(f"Inference took {time.time() - start :.2f} seconds")

    predictions.to_csv(output_path, index=False)
    logger.info(f"Predictions saved to {output_path}")
    
    return predictions


def upload_to_s3_via_cli(local_filename: str, s3_destination: str) -> None:
    subprocess.run(["aws", "s3", "cp", local_filename, s3_destination])


if __name__ == "__main__":
    bucket_name = sys.argv[1]
    s3_filename = sys.argv[2]
    local_filename = sys.argv[3]
    output_path = sys.argv[4]
    model_id = sys.argv[5] if len(sys.argv) > 5 else EXAMPLE_MODEL_ID

    read_input_from_s3(bucket_name, s3_filename, local_filename)
    predictions = generate_predictions(local_filename, output_path, model_id)
    
    # Construct S3 destination path
    input_sha = os.environ.get('SHA')
    numerator = int(os.environ.get('numerator'))
    s3_destination = f"s3://{bucket_name}/out/{model_id}/{input_sha}/{input_sha}_{numerator}.csv"
    
    # Upload predictions to S3 using AWS CLI
    upload_to_s3_via_cli(output_path, s3_destination)
