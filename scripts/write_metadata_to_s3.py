import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from precalculator.write import write_metadata_end, write_metadata_start

dotenv_path = Path("./config/stack.env")  # relative to root of git repo
load_dotenv(dotenv_path=dotenv_path)

BUCKET_NAME = str(os.getenv("S3_BUCKET_NAME"))
DYNAMODB_TABLE_NAME = str(os.getenv("DYNAMODB_TABLE_NAME"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="Metadata", description="Write Ersilia model inference pipeline metadata to S3"
    )

    parser.add_argument("model_id", help="ID of the Ersilia model for which predictions are to be written", type=str)

    parser.add_argument(
        "--pipeline_start", help="Timestamp for when the pipeline run started", type=int, required=False
    )

    parser.add_argument("--pipeline_end", help="Timestamp for when the pipeline run ended", type=int, required=False)

    return parser


def get_metadata_s3_uri(metadata_key: str) -> str:
    return os.path.join("s3://", BUCKET_NAME, metadata_key)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    metadata_key = f"meta/{args.model_id}.json"
    s3_uri = get_metadata_s3_uri(metadata_key)

    if args.pipeline_start:
        write_metadata_start(BUCKET_NAME, args.pipeline_start, metadata_key)
    elif args.pipeline_end:
        write_metadata_end(DYNAMODB_TABLE_NAME, BUCKET_NAME, args.model_id, args.pipeline_end, metadata_key, s3_uri)
