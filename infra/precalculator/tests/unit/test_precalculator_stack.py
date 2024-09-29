import aws_cdk as core
import aws_cdk.assertions as assertions

from precalculator.precalculator_stack import PrecalculatorStack

import os
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path("../../config/settings.env")
load_dotenv(dotenv_path=dotenv_path)

ATHENA_DATABASE = str(os.getenv("ATHENA_DATABASE"))
ATHENA_PREDICTION_TABLE = str(os.getenv("ATHENA_PREDICTION_TABLE"))


# example tests. To run these tests, uncomment this file along with the example
# resource in precalculator/precalculator_stack.py
def test_table_created():
    app = core.App()
    stack = PrecalculatorStack(app, "precalculator")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties(
        "AWS::Athena::Table", {"DatabaseName": ATHENA_DATABASE, "TableName": ATHENA_PREDICTION_TABLE}
    )
