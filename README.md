# Ersilia Model Precalculation Pipeline
### A collaboration between [GDI](https://github.com/good-data-institute) and Ersilia

This repository contains code and github workflows for precalculating and storing Ersilia model predictions in AWS.

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started working on this repo.

## Github Actions Workflows

### Prediction

During this workflow, we call the Ersilia model hub for a given model ID and generate predictions on the reference library. The predictions are saved as CSV files in S3.

### Serving

This workflow reads the generated predictions from S3, validates and formats the data, then finally writes it in batches to DynamoDB.

### Full Precalculation Pipeline

The full pipeline calls the predict and serve actions in sequence. Both jobs are parallelised across up to 50 workers as they are both compute-intensive processes.
