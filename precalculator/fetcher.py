import awswrangler as wr
import pandas as pd

from precalculator.config import DataLakeConfig


class PredictionFetcher:
    def __init__(self, config: DataLakeConfig, user_id: str, request_id: str, models: list[str]):
        self.config = config
        self.user_id = user_id
        self.request_id = request_id
        # TODO: decide on multi model implementation, for now assume a list of 1 model ID
        self.models = models

    def check_availability(self) -> str:
        # TODO: figure out how we are going to check for available models
        # talk to Nicholas about the order of events here
        return f"{self.models} available in prediction store"

    def fetch(self, path_to_input: str) -> pd.DataFrame:
        input_df = self._read_input_data(path_to_input)

        try:
            self._write_inputs_s3(input_df)
        except Exception as e:
            print(f"error {e}")
            raise (e)

        try:
            output_df = self._read_predictions_from_s3()
        except Exception as e:
            print(f"error {e}")
            raise (e)

        return output_df

    def _read_input_data(self, path_to_input: str) -> pd.DataFrame:
        """Reads input CSV from user and processes it for writing to the data lake

        Args:
            path_to_input (str): location of input CSV

        Returns:
            pd.DataFrame: formatted data frame
        """

        # expect just a list of SMILEs with no header
        df = pd.read_csv(path_to_input, header=None)
        df.rename({0: "smiles"}, inplace=True)

        # TODO: validate smiles?
        ## por ejemplo -----------------------------------------
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
        wr.s3.to_parquet(
            df=input_df,
            path=f"{self.config.s3_prefix}/",
            dataset=True,
            database=self.config.athena_database,
            table=self.config.athena_request_table,
            partition_cols=["user_id", "request_id"],
        )

    def _read_predictions_from_s3(self) -> pd.DataFrame:
        query = f"""
            select
                p.key,
                p.input,
                p.mw
            from
                predictions p
                inner join requests r
                    on p.input = r.smiles
            where
                r.request_id = '{self.request_id}'
                and r.user_id = '{self.user_id}'
                and p.model_id = '{self.models[0]}'
        """

        df_out = wr.athena.read_sql_query(query, database=self.config.athena_database)

        return df_out
