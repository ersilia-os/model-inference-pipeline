import boto3
import logging
import os
import sys
import awswrangler as wr


def handler(event: dict, context: dict) -> dict:
    """Fetch predictions

    Args:
        request_id (str): _description_
        model_id (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    request_id = event["queryStringParameters"]["requestid"]
    model_id = event["queryStringParameters"]["modelid"]

    fetcher = PredictionFetcher(request_id, model_id)
    path_to_input = fetcher.get_s3_input_location()
    output_s3_url = fetcher.fetch(path_to_input)
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": output_s3_url}


# TODO: merge PredictionFetchers in model-inference-pipeline repo and
# maybe create deployment package
class PredictionFetcher:
    def __init__(self, request_id: str, model_id: str, dev: bool = False):
        self.request_id = request_id
        self.model_id = model_id
        self.dev = dev

        self.logger = logging.getLogger("PredictionFetcher")
        self.logger.setLevel(logging.INFO)

        if self.dev:
            logging.basicConfig(stream=sys.stdout, level=logging.INFO)
            logging.getLogger("botocore").setLevel(logging.WARNING)

    def get_s3_input_location(self) -> str:
        return os.path.join(
            "s3://",
            os.environ.get("BUCKET_NAME"),
            os.environ.get("S3_UPLOAD_PREFIX"),
            self.model_id,
            f"{self.request_id}.csv",
        )

    def fetch(self, path_to_input: str):
        logger = self.logger

        logger.info("reading and formatting input data")
        input_df = self._read_input_data(path_to_input)
        logger.info(input_df.head())

        logger.info("writing input to athena")
        self._write_inputs_s3(input_df)

        logger.info("fetching outputs from athena")
        output_df = self._read_predictions_from_s3()

        logger.info("storing outputs into s3")
        self._write_outputs_s3(output_df)

        logger.info("returning presigned url")
        output_presigned_url = self._get_output_presigned_url()

        return output_presigned_url

    def _read_input_data(self, path_to_input: str):
        """Reads input CSV from user and processes it for writing to the data lake

        Args:
            path_to_input (str): location of input CSV

        Returns:
            pd.DataFrame: formatted data frame
        """

        # expect just a list of SMILEs with no header
        df = wr.s3.read_csv(path=path_to_input, header=None)
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

        return df[["smiles", "request_id"]]

    def _write_inputs_s3(self, input_df) -> None:
        # TODO: deduplicate repeated inputs
        wr.s3.to_parquet(
            df=input_df,
            path=os.path.join(
                "s3://",
                os.environ.get("BUCKET_NAME"),
                os.environ.get("S3_INPUT_PREFIX"),
            ),
            dataset=True,
            database=os.environ.get("ATHENA_DATABASE"),
            table=os.environ.get("ATHENA_REQUEST_TABLE"),
            partition_cols=["request_id"],
        )

    def _read_predictions_from_s3(self):
        # TODO: remove DISTINCT after inputs have been deduplicated (see TODO in _write_inputs_s3)
        query = f"""
            with request as (
                select distinct
                    *
                from
                    requests
                where
                    request_id = '{self.request_id}'
            )
            select distinct
                p.model_id,
                p.input_key,
                p.smiles,
                p.output
            from
                predictions p
                left join request r
                    on p.smiles = r.smiles
            where
                p.model_id = '{self.model_id}'
        """

        df_out = wr.athena.read_sql_query(query, database=os.environ.get("ATHENA_DATABASE"))

        return df_out

    def _write_outputs_s3(self, output_df):
        """Writes outputs to S3

        Args:
            output_df (pd.DataFrame)

        Returns:
            pd.DataFrame: formatted data frame
        """
        wr.s3.to_csv(
            df=output_df,
            path=os.path.join(
                "s3://",
                os.environ.get("BUCKET_NAME"),
                os.environ.get("S3_OUTPUT_PREFIX"),
                f"{self.model_id}",
                f"{self.request_id}.csv",
            ),
            index=False,
        )

    def _get_output_presigned_url(self):
        """Writes outputs to S3

        Args:
            None

        Returns:
            presigned_url (str): Pre-signed URL to download precalculations as csv
        """
        s3_client = boto3.client("s3")
        try:
            presigned_url = s3_client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": os.environ.get("BUCKET_NAME"),
                    "Key": f"{os.environ.get('S3_OUTPUT_PREFIX')}/{self.model_id}/{self.request_id}.csv",
                },
                ExpiresIn=3600,
            )
            return presigned_url
        except Exception as e:
            self.logger.info(f"Error generating presigned URL: {e}")
