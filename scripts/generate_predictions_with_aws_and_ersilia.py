import argparse
import logging
import os
import subprocess

import awswrangler as wr
import boto3
import pandas as pd

TEST_ENV = {
    "MODEL_ID": "eos2zmb",
    "SHA": "1234",
    "numerator": 1,
    "denominator": 2,
    "sample-only": 10,
    "GITHUB_REPOSITORY": "precalculations-bucket",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def split_csv(input_path: str, numerator: int, denominator: int) -> str:
    df = pd.read_csv(input_path)

    total_length = len(df)
    chunk_size = total_length // denominator

    start_row = (numerator - 1) * chunk_size
    end_row = start_row + chunk_size

    df = df.iloc[start_row:end_row]
    df.to_csv("input.csv", index=False)

    return "input.csv"


def fetch_input_from_s3(bucket_name: str, filename: str, local_filename: str) -> None:
    s3 = boto3.client("s3")
    s3.download_file(bucket_name, filename, local_filename)
    logger.info(f"Downloaded {filename} from S3")


def upload_to_s3_via_cli(local_filename: str, s3_destination: str) -> None:
    subprocess.run(["aws", "s3", "cp", local_filename, s3_destination])


parser = argparse.ArgumentParser()
parser.add_argument("-e", "--env", choices=["dev", "ci", "prod"], default="dev", help="Specify environment")

if __name__ == "__main__":
    args = parser.parse_args()

    env_source = TEST_ENV if args.env == "dev" else os.environ
    logger.info(f"environment: {args.env}")

    model_id = env_source.get("MODEL_ID")  # type: ignore
    sha = env_source.get("SHA")  # type: ignore
    numerator = int(env_source.get("numerator"))  # type: ignore
    denominator = int(env_source.get("denominator"))  # type: ignore
    sample_only = env_source.get("sample-only")  # type: ignore
    bucket_name = env_source.get("GITHUB_REPOSITORY").replace("/", "-")  # type: ignore

    # sample-only defines size of smaller reference library files from s3
    input_filename = f"reference_library_{sample_only}.csv" if sample_only else "reference_library.csv"

    logger.info(f"fetching input {bucket_name, input_filename} from s3")
    fetch_input_from_s3(bucket_name, input_filename, input_filename)

    partitioned_input = split_csv(input_filename, numerator, denominator)

    logger.info(f"calling ersilia for model {model_id}")
    subprocess.run([".venv/bin/ersilia", "-v", "serve", model_id])  # type: ignore
    subprocess.run([".venv/bin/ersilia", "-v", "run", "-i", partitioned_input, "-o", "output.csv"])

    s3_destination = f"s3://precalculations-bucket/out/{model_id}/{sha}/{sha}_{numerator - 1:04d}.csv"

    logger.info("postprocessing predicitons")
    df = pd.read_csv("output.csv")
    columns_to_use = df.columns[-2:]
    output = df[columns_to_use].to_dict(orient="records")
    df["output"] = output
    df["model_id"] = model_id
    df = df[["key", "input", "output", "model_id"]]
    df = df.rename(columns={"key": "input_key", "input": "smiles"})

    logger.info("writing predicitons to s3")

    wr.s3.to_parquet(
        df=df,
        path=os.path.join(
            "s3://",
            "precalculations-bucket",
            "predictions",
        ),
        dataset=True,
        database="precalcs_test",
        table="predictions",
        partition_cols=["model_id"],
    )
