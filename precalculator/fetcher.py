import logging
import os
import sys

import awswrangler as wr
import pandas as pd

from config.app import DataLakeConfig


class PredictionFetcher:
    def __init__(self, config: DataLakeConfig, user_id: str, request_id: str, model_id: str, dev: bool = False):
        self.config = config
        self.user_id = user_id
        self.request_id = request_id
        self.model_id = model_id
        self.dev = dev

        self.logger = logging.getLogger("PredictionFetcher")
        self.logger.setLevel(logging.INFO)

        if self.dev:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO)
            logging.getLogger("botocore").setLevel(logging.WARNING)

    def check_availability(self) -> str:
        # TODO: figure out how we are going to check for available models
        # talk to Nicholas about the order of events here

        # we need to reference the metadata table here, it can also be an athena table

        return f"{self.model_id} available in prediction store"

    def get_s3_input_location(self) -> str:
        return os.path.join(
            "s3://", self.config.s3_bucket_name, self.config.s3_upload_prefix, self.model_id, self.request_id, "*"
        )

    def fetch(self, path_to_input: str) -> pd.DataFrame:
        logger = self.logger

        logger.info("reading and formatting input data")
        input_df = self._read_input_data(path_to_input)

        logger.info("writing input to athena")
        self._write_inputs_s3(input_df)

        logger.info("fetching outputs from athena")
        output_df = self._read_predictions_from_s3()

        return output_df

    def _validate_input(self, path_to_input: str) -> None:
        # check that it is a CSV/text file
        # check that size is within reasonable bounds
        # ...
        # if passes all tests, return True
        pass

    def _read_input_data(self, path_to_input: str) -> pd.DataFrame:
        """Reads input CSV from user and processes it for writing to the data lake

        Args:
            path_to_input (str): location of input CSV

        Returns:
            pd.DataFrame: formatted data frame
        """

        # expect just a list of SMILEs with no header
        df = pd.read_csv(path_to_input, header=None)
        df.rename(columns={0: "smiles"}, inplace=True)

        # TODO: validate smiles?
        ## for example -----------------------------------------
        # cid = CompoundIdentifier()
        # valid_smiles = df.apply(lambda x: cid._is_smiles(x))

        # df_invalid = df[~valid_smiles]
        # # generate invalid request summary

        # df = df[valid_smiles]
        # ------------------------------------------------------

        df["request_id"] = self.request_id
        df["user_id"] = self.user_id

        return df[["user_id", "request_id", "smiles"]]

    def _write_inputs_s3(self, input_df: pd.DataFrame) -> None:
        # TODO: deduplicate repeated inputs
        wr.s3.to_parquet(
            df=input_df,
            path=os.path.join(
                "s3://",
                self.config.s3_bucket_name,
                self.config.s3_input_prefix,
            ),
            dataset=True,
            database=self.config.athena_database,
            table=self.config.athena_request_table,
            partition_cols=["user_id", "request_id"],
        )

    def _read_predictions_from_s3(self) -> pd.DataFrame:
        query = f"""
            with request as (
                select distinct
                    *
                from
                    requests
                where
                    request_id = '{self.request_id}'
                    and user_id = '{self.user_id}'
            )

            select
                p.model_id,
                p.input_key,
                p.smiles,
                p.output
            from
                predictions p
                left join request r
                    on p.smiles = r.smiles
            where
                and p.model_id = '{self.model_id}'
        """

        df_out = wr.athena.read_sql_query(query, database=self.config.athena_database)

        return df_out
