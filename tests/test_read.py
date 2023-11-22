import pandas as pd

from ersilia_precalc_poc.models import Prediction
from ersilia_precalc_poc.read import get_predictions_from_dataframe

fixture = pd.DataFrame(
    {
        "key": ["PCQFQFRJSWBMEL-UHFFFAOYSA-N"],
        "input": ["COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21"],
        "mw": [283.239],
    }
)


def test_read():
    actual_preds = get_predictions_from_dataframe("modelid", fixture)

    expected_preds = [
        Prediction(
            model_id="modelid",
            input_key="PCQFQFRJSWBMEL-UHFFFAOYSA-N",
            smiles="COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21",
            output=[283.239],
        )
    ]

    assert actual_preds == expected_preds
