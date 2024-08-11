import logging
import time
import os
import subprocess
import sys
from typing import List
import boto3
import pandas as pd
import awswrangler as wr

EXAMPLE_MODEL_ID = "eos2zmb"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def split_csv(input_path: str, numerator: int, denominator: int) -> str:
    df = pd.read_csv(input_path)

    total_length = len(df)
    print(total_length)

    chunk_size = total_length // denominator
    print(chunk_size)


    start_row = (numerator-1) * chunk_size
    end_row = start_row + chunk_size

    print(start_row, end_row)

    df = df.iloc[start_row:end_row]
    df.to_csv("input.csv", index=False)

    return "input.csv"


def fetch_input_from_s3(bucket_name: str, filename: str, local_filename: str) -> None:
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, filename, local_filename)
    logger.info(f"Downloaded {filename} from S3")

def upload_to_s3_via_cli(local_filename: str, s3_destination: str) -> None:
    subprocess.run(["aws", "s3", "cp", local_filename, s3_destination])


TEST_ENV = {
    "MODEL_ID": "eos2zmb",
    "SHA": "1234",
    "numerator": 1,
    "denominator": 2,
    "sample-only": 10,
    "GITHUB_REPOSITORY": "precalculations-bucket",
}

if __name__ == "__main__":

    if sys.argv[1] == "dev":
        env_source = TEST_ENV
    else:
        env_source = os.environ
    
    # Reading inputs from environment variables
    model_id = env_source.get('MODEL_ID')
    sha = env_source.get('SHA')
    numerator = int(env_source.get('numerator'))
    denominator = int(env_source.get('denominator'))
    sample_only = env_source.get('sample-only')
    
    # Construct bucket name based on GitHub repository
    bucket_name = env_source.get('GITHUB_REPOSITORY').replace('/', '-')
    
    # Determine input filename based on sample-only flag
    input_filename = f"reference_library_{sample_only}.csv" if sample_only else "reference_library.csv"
    
    # Fetch input data from S3
    fetch_input_from_s3(bucket_name, input_filename, input_filename)
    
    # # Split the input file into partitions
    partitioned_input = split_csv(input_filename, numerator, denominator)
    
    # # Determine which partition this worker should process
    # partition_file = partition_files[numerator - 1]  
    output_path_template = f"../{sha}_{numerator - 1:04d}.csv"
    
    # Generate predictions and save locally
    # predictions = generate_predictions(input_filename, output_path_template, model_id, sha, numerator)

    subprocess.run(["ersilia", "serve", model_id])
    subprocess.run(["ersilia", "run", "-i", partitioned_input, "-o", "output.csv"])


    # Construct S3 destination path
    s3_destination = f"s3://precalculations-bucket/out/{model_id}/{sha}/{sha}_{numerator - 1:04d}.csv"

    # TODO: postprocess predictions

    # TODO: write preds to s3 with aws wrangler

