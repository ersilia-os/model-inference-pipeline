import argparse
import logging
import os
import sys

from config.app import DataLakeConfig, WorkerConfig
from precalculator.writer import PredictionWriter

TEST_ENV = {
    "INPUT_MODEL_ID": "eos2zmb",
    "INPUT_SHA": "1234",
    "INPUT_NUMERATOR": 1,
    "INPUT_DENOMINATOR": 2,
    "INPUT_SAMPLE_ONLY": 10,
}

logger = logging.getLogger("GeneratePredictionsScript")
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("-e", "--env", choices=["dev", "ci", "prod"], default="dev", help="Specify environment")

if __name__ == "__main__":
    args = parser.parse_args()

    env_source = os.environ
    dev = False

    if args.env == "dev":
        env_source = TEST_ENV  # type: ignore
        dev = True

    logger.info(f"Environment: {args.env}")

    logger.info("Setting up writer configuration")
    model_id = str(env_source.get("INPUT_MODEL_ID"))  # type: ignore
    sha = env_source.get("INPUT_SHA")  # type: ignore
    numerator = int(env_source.get("INPUT_NUMERATOR"))  # type: ignore
    denominator = int(env_source.get("INPUT_DENOMINATOR"))  # type: ignore
    sample_only = env_source.get("INPUT_SAMPLE_ONLY")  # type: ignore

    data_config = DataLakeConfig()  # type: ignore
    worker_config = WorkerConfig(  # type: ignore
        git_sha=sha,  # type: ignore
        denominator=denominator,  # type: ignore
        numerator=numerator,  # type: ignore
        sample=sample_only,
    )

    logger.info(
        "Configured writer with following DataLake settings: \n%s",
        "\n".join(f"{k}: {v}" for k, v in data_config.model_dump().items()),
    )

    logger.info(
        "Configured writer with following Worker settings: \n%s",
        "\n".join(f"{k}: {v}" for k, v in worker_config.model_dump().items()),
    )

    writer = PredictionWriter(data_config=data_config, worker_config=worker_config, model_id=model_id, dev=dev)

    input_file = writer.fetch()

    output_file = writer.predict(input_file)

    df_predictions = writer.postprocess(output_file)

    writer.write_to_lake(df_predictions)
