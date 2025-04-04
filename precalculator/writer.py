# import json
import logging
import os
import subprocess
import sys

import awswrangler as wr
import boto3
import pandas as pd

from config.app import DataLakeConfig, WorkerConfig
from precalculator.models import (
    Prediction,
    validate_dataframe_schema,
)

INPUT_NAME = "reference_library"
INPUT_FILE_NAME = f"{INPUT_NAME}.csv"
PROCESSED_FILE_NAME = "input.csv"
OUTPUT_FILE_NAME = "output.csv"


class PredictionWriter:
    def __init__(self, data_config: DataLakeConfig, worker_config: WorkerConfig, model_id: str, dev: bool):
        self.data_config = data_config
        self.worker_config = worker_config
        self.model_id = model_id
        self.s3 = boto3.client("s3")
        self.dev = dev

        self.logger = logging.getLogger("PredictionWriter")
        self.logger.setLevel(logging.INFO)

        if self.dev:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO)
            logging.getLogger("botocore").setLevel(logging.WARNING)

    # def write_metadata(self, bucket: str, metadata_key: str, metadata: Metadata) -> None:
    #     self.s3.put_object(Bucket=bucket, Key=metadata_key, Body=json.dumps(metadata.model_dump_json()))

    def fetch(self) -> str:
        """Fetch and split inputs for this worker, ready to pass to Ersilia CLI"""
        logger = self.logger

        if self.worker_config.sample:
            input_filename = f"{INPUT_NAME}_{self.worker_config.sample}.csv"
        else:
            input_filename = INPUT_FILE_NAME

        self.s3.download_file(
            self.data_config.s3_bucket_name,
            input_filename,
            INPUT_FILE_NAME,
        )

        logger.info(f"Downloaded {input_filename} from S3")

        partition_metadata = self._split_csv()

        logger.info(f"Partitioned rows {partition_metadata[0]} to {partition_metadata[1]}")

        return PROCESSED_FILE_NAME

    def predict(self, input_file_path: str) -> str:
        """Calls Ersilia CLI to generate predictions for provided input CSV.

        This method gets Ersilia to pull and serve the relevant model container.

        Args:
            input_file_path (str): path to input CSV
        """
        logger = self.logger

        logger.info(f"Calling Ersilia CLI for model {self.model_id}")

        subprocess.run([".venv/bin/ersilia", "-v", "fetch", self.model_id, "--from_github"])  # type: ignore
        subprocess.run([".venv/bin/ersilia", "-v", "serve", self.model_id, "--no-cache"])  # type: ignore
        subprocess.run([".venv/bin/ersilia", "-v", "run", "-i", input_file_path, "-o", OUTPUT_FILE_NAME])

        return OUTPUT_FILE_NAME

    def postprocess(self, ersilia_output_path: str) -> pd.DataFrame:
        """Postprocessing for output file from Ersilia CLI

        Args:
            ersilia_output_path (str): location of output CSV

        Returns:
            pd.DataFrame: postprocessed dataframe of outputs
        """

        logger = self.logger
        logger.info("Postprocessing outputs from Ersilia model")

        df = pd.read_csv(ersilia_output_path)

        output_cols = df.columns[2:]
        output_records = df[output_cols].to_dict(orient="records")

        df["output"] = output_records
        df["model_id"] = self.model_id
        df = df[["key", "input", "output", "model_id"]]
        df = df.rename(columns={"key": "input_key", "input": "smiles"})

        return df

    def write_to_lake(self, outputs: pd.DataFrame) -> None:
        validate_dataframe_schema(outputs, Prediction)  # type: ignore

        wr.s3.to_parquet(
            df=outputs,
            path=os.path.join(
                "s3://",
                self.data_config.s3_bucket_name,
                self.data_config.athena_prediction_table,
            ),
            dataset=True,
            database=self.data_config.athena_database,
            table=self.data_config.athena_prediction_table,
            partition_cols=["model_id"],
        )

    def _split_csv(self) -> tuple[int, int]:
        """Partition CSV file such that the worker has the correct set of rows to predict on"""
        df = pd.read_csv(INPUT_FILE_NAME)

        total_length = len(df)
        chunk_size = total_length // self.worker_config.denominator

        start_row = (self.worker_config.numerator - 1) * chunk_size
        end_row = start_row + chunk_size

        df = df.iloc[start_row:end_row]
        df.to_csv(PROCESSED_FILE_NAME, index=False)

        return start_row, end_row
