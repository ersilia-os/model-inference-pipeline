import argparse
import os

import pandas as pd

from ersilia_precalc_poc.read import get_predictions_from_dataframe
from ersilia_precalc_poc.test import write_test_rows
from ersilia_precalc_poc.write import write_precalcs_batch_writer

# aws region: eu-central-1
# dynamodb table name: precalculations-poc

# hard code for now
# TODO: use .env file for config
DYNAMODB_TABLE_NAME = "precalculations-poc"
PREDICTIONS_S3_PREFIX = "s3://isaura-bucket/out/"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="Serving", description="Write Ersilia model predictions to DynamoDB")

    parser.add_argument(
        "model_id",
        help="ID of the Ersilia model for which predictions are to be written",
        type=str,
    )

    parser.add_argument(
        "sha",
        help="SHA of the inference pipeline run which generated the predictions we wish to serve",
        type=str,
    )

    parser.add_argument(
        "partition",
        help="Number of the partition to be written, this is the suffix of the CSV in S3",
        default=0,
    )

    parser.add_argument(
        "--local",
        help="dev option for testing with locally stored files instead of S3",
        required=False,
        action="store_true",
    )

    return parser


def get_predictions_s3_uri(model_id: str, sha: str, partition: int) -> str:
    return os.path.join(PREDICTIONS_S3_PREFIX, model_id, f"{sha}_{partition:0>4d}.csv")


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    # default test fixture, just chuck some records into the table
    if args.model_id == "test":
        write_test_rows(DYNAMODB_TABLE_NAME)

    else:
        if args.local:
            df = pd.read_csv(f"{args.sha}.csv")
        else:
            # look for csv at provided path
            df = pd.read_csv(get_predictions_s3_uri(args.model_id, args.sha, int(args.partition)))

        print(f"read {len(df)} predictions from {args.model_id}")

        pred_data = get_predictions_from_dataframe(args.model_id, df)

        write_precalcs_batch_writer(DYNAMODB_TABLE_NAME, pred_data)
