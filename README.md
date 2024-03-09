# Spark & Data Engineering Challenge

## Setup
This package was build using [poetry](https://github.com/python-poetry/poetry).
In order to install all the dependencies and run the processing pipeline run following commands:

`poetry install` \
`poetry shell` \
`python spark_challenge/process_actions.py -events_path YOUR_EVENTS_PATH`

The main logic of the pipeline can be find in `spark_challenge/process_actions.py`. The main function runs all the required steps, the output of each function can also be found in the comments.

### Testing
Each processing function is tested (see the test file `tests/test_process_actions.py`). To run the test, just run `pytest` from the package root directory.

### Code formatting
In order to ensure that the code is formatted accordingly to the modern standards, the [black formatter](https://github.com/psf/black) is used. Also, we use the [pre-commit](https://pre-commit.com/) hooks to automaticly format files, e.g. fix end-of-file, trailing-whitespace or yaml formatting. The the formatting step with black is also a part of it (see the hook details under `.pre-commit-config.yaml`). In order to activate the pre-commit hooks, run `pre-commit install`. Then, you can run it with `pre-commit run --all-files` or by committing something.
