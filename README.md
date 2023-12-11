# Ersilia Model Precalculation Pipeline

[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/release/python-370/) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?logo=Python&logoColor=white)](https://github.com/psf/black)



This repository contains code and github workflows for precalculating and storing Ersilia model predictions in AWS.

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started working on this repo.

## Using the Batch Inference Pipeline

### Triggering a pipeline run

The workflow "Run Inference in Parallel" can be triggered from the GitHub UI. `Actions` > `Run Inference in Parallel` > `Run workflow`. Then, simply enter the ID of the Ersilia Model Hub model that you want to run.

### Querying the precalculation database

Predictions end up being written to DynamoDB, where they can be retrieved via the precalculations API endpoint. Find the endpoint URL on AWS in the API Gateway console.

To query the endpoint, we need:

1. an API key
2. Ersilia model ID for desired model
3. InChiKey(s) of desired inputs


The request body has the following schema:
```
{
  "$schema": "http://json-schema.org/draft/2020-12/schema#",
  "type": "object",
  "properties": {
    "modelId": {
      "type": "string"
    },
    "inputKeyArray": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  },
  "required": ["modelId", "inputKeyArray"]
}
```
example:
```
{
    "modelId": "eos92sw",
    "inputKeyArray":[
            "PCQFQFRJSWBMEL-UHFFFAOYSA-N",
            "MRSBJIAZTHGJAP-UHFFFAOYSA-N"
        ]
}
```

## Architecture and Cloud Infrastructure

![architecture diagram](docs/architecture-diagram.png)

Key components:
- inference and serving compute; GitHub Actions workers
- prediction bulk storage; S3 Bucket
- prediction database; DynamoDB
- serverless API; Lambda + API Gateway

All AWS components are managed via IaC with [AWS CDK](https://aws.amazon.com/cdk/). See [infra/precalculator](infra/precalculator/README.md) for details on how to validate and deploy infrastructure for this project.

## Github Actions Workflows

### Prediction

During this workflow, we call the Ersilia model hub for a given model ID and generate predictions on the reference library. The predictions are saved as CSV files in S3.

This works by pulling the [Ersilia Model Hub](https://github.com/ersilia-os/ersilia) onto a GitHub Ubuntu worker and running inference for a slice of the reference library. Predictions are saved to S3 via the AWS CLI.

### Serving

This workflow reads the generated predictions from S3, validates and formats the data, then finally writes it in batches to DynamoDB.

This uses the python package `precalculator` developed in this repo. The package includes:

- validation of input data with `pydantic` and `pandera`
- testing with `pytest`
- batch writing to DynamoDB with `boto3`

### Full Precalculation Pipeline

The full pipeline calls the predict and serve actions in sequence. Both jobs are parallelised across up to 50 workers as they are both compute-intensive processes.

`predict-parallel.yml` implements this FULL pipeline ("Run Inference in Parallel") in a manner which avoids the 6-hour time out limit individual workflows.


---

##### A collaboration between [GDI](https://github.com/good-data-institute) and [Ersilia](https://github.com/ersilia-os)

<div id="top"></div>
<img src="https://avatars.githubusercontent.com/u/75648991?s=200&v=4" height="50" style="margin-right: 20px">
<img src="https://raw.githubusercontent.com/ersilia-os/ersilia/master/assets/Ersilia_Plum.png" height="50">
