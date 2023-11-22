## Requirements
- Linux or MacOS
- python 3.10 installation (recommend using pyenv)

Set up virtual environment with `make install`, and activate it with `source .venv/bin/activate`

We use `poetry` for virtual env creation and dependency management.  
We use `black` and `ruff` for python code formatting and linting.  
We use `mypy` for python type checking.  
We use `pytest` for unit and integration tests.


### Generating example pre-calculations
#### Fetch input data
Manually download the [reference file](https://github.com/ersilia-os/groverfeat/raw/main/data/reference_library.csv) and save it to a directory of this repo named `data/`. The file is ~100MB.

You can take a subset of this data or run the whole thing.

Can use scripts/generate_predictions.py, or just use the ersilia CLI.

Requirements for the model hub (ersilia CLI)
- git lfs
- docker

```
python scripts/generate_predictions.py <path_to_input> <path_of_output>
```
