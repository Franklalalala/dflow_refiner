# dflow_refiner

Dflow OP for properly integrating multi-calculators.

This OP is the default output of the
[dflow OP cutter](https://github.com/deepmodeling/dflow-op-cutter),
intended to help developers get started with their dflow OPs.

## Repository contents

* [`.github/`](.github/): [Github Actions](https://github.com/features/actions) configuration
  * [`ci.yml`](.github/workflows/ci.yml): runs tests at every new commit
  * [`publish-on-pypi.yml`](.github/workflows/publish-on-pypi.yml): automatically deploy git tags to PyPI - just generate a [PyPI API token](https://pypi.org/help/#apitoken) for your PyPI account and add it to the `PYPI_API_TOKEN` secret of your github repository
  * [`publish-on-dockerhub.yml`](.github/workflows/publish-on-dockerhub.yml): automatically build docker image and push to Docker Hub - just add the `DOCKER_USERNAME` and `DOCKER_PASSWORD` secrets of your github repository
* [`refiner/`](refiner/): The main source code of the OP package
* [`examples/`](examples/): An example of how to submit a workflow using this OP
* [`tests/`](tests/): Basic regression tests using the [pytest](https://docs.pytest.org/en/latest/) framework.
* [`VERSION`](VERSION): Current version
* [`Dockerfile`](Dockerfile): Dockerfile for building docker image
* [`.gitignore`](.gitignore): Telling git which files to ignore
* [`LICENSE`](LICENSE): License for your OP
* [`README.md`](README.md): This file
* [`pyproject.toml`](pyproject.toml): Python package metadata for registration on [PyPI](https://pypi.org/)

## Installation

```shell
pip install pydflow, ase, pandas
pip install dflow-refiner
docker pull franklalalala/py_autorefiner
```
