import time

import boto3

test_data = [
    {
        "model_id": "examplemodel",
        "input_key": "PCQFQFRJSWBMEL-UHFFFAOYSA-N",
        "smiles": "COC(=O)C1=CC=CC2=C1C(=O)C1=CC([N+](=O)[O-])=CC=C21",
        "output": "283.239",
    },
    {
        "model_id": "examplemodel",
        "input_key": "MRSBJIAZTHGJAP-UHFFFAOYSA-N",
        "smiles": "CN(C)CCC1=CN(C)C2=CC=C(O)C=C12",
        "output": "218.3",
    },
]


def write_test_rows(
    ddb_table_name: str,
) -> None:
    # default test fixture, just chuck some records into the table
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(ddb_table_name)
    data = test_data

    with table.batch_writer() as writer:
        for item in data:
            writer.put_item(
                Item={
                    "PK": f"INPUTKEY#{item['input_key']}",
                    "SK": f"MODELID#{item['model_id']}",
                    "Smiles": item["smiles"],
                    "Precalculation": item["output"],
                    "Timestamp": str(time.time()),
                }
            )
            print(f"wrote record with key {item['input_key']}")
