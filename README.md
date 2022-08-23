# dflow_refiner

Dflow OP for properly integrating multi-calculators.

This OP is the default output of the
[dflow OP cutter](https://github.com/deepmodeling/dflow-op-cutter),
intended to help developers get started with their dflow OPs.

## Repository contents

* [`.github/`](.github/): [Github Actions](https://github.com/features/actions) configuration
  * [`publish-on-pypi.yml`](.github/workflows/publish-on-pypi.yml): automatically deploy git tags to PyPI when version changed
* [`dflow_refiner/`](dflow_refiner/): The main source code of the OP package
* [`examples/`](examples/): An example of how to submit a workflow using this OP
  * [`unit_test/`](examples/unit_test): Detailed test of functions and OPs.
  * [`miniAutoSteper/`](examples/miniAutoSteper): Simplified version of [`AutoSteper`]([Franklalalala/AutoSteper: Automated Stepwise Addition Procedure for Extrafullerene. (github.com)](https://github.com/Franklalalala/AutoSteper))
  * [`conformation_search_test/`](examples/conformation_search_test): Automated conformation search with `dflow_refiner`
* [`VERSION`](VERSION): Current version
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

## Contact me

E-mail: 1660810667@qq.com
