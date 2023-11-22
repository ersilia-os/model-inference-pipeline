import pandas as pd
import pytest
from pandera import check_types
from pandera.errors import SchemaError
from pandera.typing import DataFrame

from ersilia_precalc_poc.models import BasePredictionSchema, Prediction


def test_prediction():
    fixture = {"model_id": "a", "input_key": "b", "smiles": "C", "output": [1]}

    prediction = Prediction(model_id="a", input_key="b", smiles="C", output=[1])

    constructed_pred = Prediction.construct(**fixture)

    assert prediction.dict() == fixture
    assert constructed_pred == prediction


@check_types
def _validate(df: DataFrame[BasePredictionSchema]) -> DataFrame[BasePredictionSchema]:
    return df


def test_implicit_validation_success():
    fixture = pd.DataFrame(
        {
            "key": ["PCQFQFRJSWBMEL-UHFFFAOYSA-N"],
            "input": ["COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21"],
            "mw": [283.239],
        }
    )

    _validate(fixture)


def test_implicit_validation_fail():
    fixture = pd.DataFrame(
        {"key": ["bad-inchi-key"], "input": ["COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21"], "mw": [283.239]}
    )

    with pytest.raises(SchemaError):
        _validate(fixture)


def test_prediction_schema_valid_df():
    fixture = pd.DataFrame(
        {
            "key": ["PCQFQFRJSWBMEL-UHFFFAOYSA-N"],
            "input": ["COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21"],
            "mw": [283.239],
        }
    )

    validated_df = BasePredictionSchema.validate(fixture)

    assert validated_df.equals(fixture)


def test_prediction_schema_invalid_df():
    not_inchi_key = pd.DataFrame(
        {"key": ["not an inchi key"], "input": ["COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21"], "mw": [283.239]}
    )

    bad_input = pd.DataFrame({"key": ["PCQFQFRJSWBMEL-UHFFFAOYSA-N"], "input": [1923], "mw": [283.239]})

    with pytest.raises(SchemaError):
        BasePredictionSchema.validate(not_inchi_key)

    with pytest.raises(SchemaError):
        BasePredictionSchema.validate(bad_input)
