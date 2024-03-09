# Spark & Data Engineering Challenge

## Setup
This package was build using [poetry](https://github.com/python-poetry/poetry).
In order to install all the dependencies and run the processing pipeline run following commands:

`poetry install` \
`poetry shell` \
`python spark_challenge/process_actions.py`

The main logic of the pipeline can be find in `spark_challenge/process_actions.py`. The main function runs all the required steps, the output of each function can also be found in the comments.

Each processing function is tested (see the test file `tests/test_process_actions.py`). To run the test, just run `pytest` from the root directory.
