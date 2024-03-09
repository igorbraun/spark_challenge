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

## Additional questions
### Spark Challenge

#### Optimization and Scalability

- **Use cluster** This challenge was developed on my local machine, which was perfectly fine for this data set size. In a production setting, one would need a spark cluster of sufficient size to process larger datasets.
- **Partitioning** In my solution, the data was grouped into time windows based on the timestamp. In order to utilize the cluster with many worker nodes, one would need to partition the data based on the date (the structure of the files would be then `/events/year/month/day`). Each day would lie on the same worker node and could so be processed in parallel.
- **Iterative processing, e.g. using Spark Structured Streaming** We could also avoid to process the whole dataset at the same time and use Spark Structured Streaming to process the dataset iteratively.


#### Testing and Deployment:
- **Integration testing** As part of this challenge, the code was tested with the unit tests. In addition, the integration tests are of course helpful and useful for testing the entire data pipeline in a larger context. To do this, you would start earlier with the ingestion step and compare the results of the whole pipelien with the tables, that you would actually expect in your lakehouse after the whole pipeline is finished.

- **Data quality checks** The data set itself was not tested in this challenge. In a production environment, it is of course essential to test the data for certain expectations, e.g. that certain columns do not contain nulls or that the columns may only contain certain values (such as Open or Closed in our case) or lie within a certain range (e.g. the timestamp is not in the future or not 500 years ago). Data quality tools such asg [great expectations](https://greatexpectations.io/) are well suitable for this purpose.

#### Bonus Challenge:
- **CI/CD Pipeline** An example CI/CD pipeline with the steps for code testing, pre-commit checks and deployment to python package registry is implemented under `.gitlab-ci.yaml`. As the name of the pipeline says, it is implemented for gitlab and not for github actions (since both provider require payed accounts to be executed, I selected the provider that I am most familiar with). For simplicity, in the current setting the package deployment runs manually. It is assumed, that the code is tagged according to the current package version (e.g. 0.1.0). At every execution of this manual step, the git tag is fetched and its minor version is incremented (0.1.0 -> 0.2.0) with a helper script (which is not a part of this repo, but basically just does the increment). If one needs to increase the major version, it can be done by tagging the source code accordingly. In more complex settings, one can also fully automate the deployment (e.g. trigger an automatic deployment every time when a feature branch is merged into master). But here, one need to discuss with the team, which workflow the team shoud follow.

- **Rollback strategies** The simplest rollback strategy in our setting would be a munual delete of the lastly deployed package from the package registry (then the latest deployed package would be the one before the critical one and it would be installed every time the cluster starts). One can also create an manual or automatic CI/CD step for this purpose using package registry API of the corresponding provider. For gitlab, one can also use the [auto rollback](https://docs.gitlab.com/ee/ci/environments/#auto-rollback), which automatically remove last deployments when a critical alert is reported.


### ETL Challange


### Data Pipeline
