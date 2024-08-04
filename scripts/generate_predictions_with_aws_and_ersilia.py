import logging
import time
import os
import subprocess
from typing import List
import boto3
import pandas as pd
from ersilia import ErsiliaModel  # type: ignore

EXAMPLE_MODEL_ID = "eos2zmb"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def split_csv(input_path: str, denominator: int) -> List[str]:
    df = pd.read_csv(input_path)
    num_chunks = denominator
    chunk_size = len(df) // num_chunks + (len(df) % num_chunks > 0)
    
    partition_files = []
    for i in range(num_chunks):
        start_row = i * chunk_size
        end_row = (i + 1) * chunk_size
        partition_df = df.iloc[start_row:end_row]
        
        partition_filename = f"partition_{i:04d}.csv"
        partition_df.to_csv(partition_filename, index=False)
        partition_files.append(partition_filename)
        
        logger.info(f"Created partition file {partition_filename}")
    
    return partition_files


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


def generate_predictions(input_path: str, output_path_template: str, model_id: str, sha: str, numerator: int) -> pd.DataFrame:
    input_items = read_input_from_file(input_path)

    with ErsiliaModel(model_id) as mdl:
        logger.info(f"Fetched model {model_id}")

        start = time.time()
        predictions = mdl.run(input_items, output="pandas")
        logger.info(f"Inference took {time.time() - start :.2f} seconds")

    output_path = output_path_template.format(sha=sha, numerator=numerator)
    predictions.to_csv(output_path, index=False)
    logger.info(f"Predictions saved to {output_path}")
    
    return predictions


def upload_to_s3_via_cli(local_filename: str, s3_destination: str) -> None:
    subprocess.run(["aws", "s3", "cp", local_filename, s3_destination])


if __name__ == "__main__":
    # Reading inputs from environment variables
    model_id = os.environ.get('MODEL_ID')
    sha = os.environ.get('SHA')
    numerator = int(os.environ.get('numerator'))
    denominator = int(os.environ.get('denominator'))
    sample_only = os.environ.get('sample-only')
    
    # Construct bucket name based on GitHub repository
    bucket_name = os.environ.get('GITHUB_REPOSITORY').replace('/', '-')
    
    # Determine input filename based on sample-only flag
    input_filename = f"reference_library_{sample_only}.csv" if sample_only else "reference_library.csv"
    
    # Fetch input data from S3
    read_input_from_s3(bucket_name, input_filename, input_filename)
    
    # Split the input file into partitions
    partition_files = split_csv(input_filename, denominator)
    
    # Determine which partition this worker should process
    partition_file = partition_files[numerator - 1]  
    output_path_template = f"../{sha}_{numerator - 1:04d}.csv"
    
    # Generate predictions and save locally
    predictions = generate_predictions(partition_file, output_path_template, model_id, sha, numerator)
    
    # Construct S3 destination path
    s3_destination = f"s3://precalculations-bucket/out/{model_id}/{sha}/{sha}_{numerator - 1:04d}.csv"
    
    # Upload predictions to S3
    upload_to_s3_via_cli(output_path_template, s3_destination)

