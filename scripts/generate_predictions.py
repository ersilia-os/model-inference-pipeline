import logging
import sys
import time
from typing import List

from ersilia import ErsiliaModel  # type: ignore

EXAMPLE_MODEL_ID = "eos2zmb"

logger = logging.Logger("logger")


def read_input_from_file(path_to_input: str = "reference_library.csv") -> List[str]:
    start = time.time()
    with open(path_to_input, "r") as file:
        contents = file.readlines()
    logger.info(f"Reading took {time.time() - start :2f} seconds")

    logger.info(f"Input file has {len(contents)} rows")

    return contents


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "prediction_output"
    model_id = sys.argv[3] if len(sys.argv) > 3 else EXAMPLE_MODEL_ID
    format = sys.argv[4] if len(sys.argv) > 4 else "csv"

    input_items = read_input_from_file(input_path)

    with ErsiliaModel(model_id) as mdl:
        logger.info(f"Fetched model {model_id}")

        start = time.time()
        predictions = mdl.run(input_items, output="pandas")
        logger.info(f"Inference took {time.time() - start :2f} seconds")

    if format == "csv":
        predictions.to_csv(output_path + ".csv")
    elif format == "parquet":
        predictions.to_parquet(output_path + ".parquet")
    else:
        print("unsupported format")
