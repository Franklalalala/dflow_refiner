# dflow_refiner

Dflow OP library to integrate multi-calculators for high-throughput molecule screening.

This OP library constructed by [dflow OP cutter](https://github.com/deepmodeling/dflow-op-cutter).

## Repository contents

* [`.github/`](.github/): [Github Actions](https://github.com/features/actions) configuration
  * [`publish-on-pypi.yml`](.github/workflows/publish-on-pypi.yml): automatically deploy git tags to PyPI when version changed
* [`dflow_refiner/`](dflow_refiner/): The main source code of the OP package.
* [`tutorial_VASP/`](./tutorial_VASP): Tutorial to build refiner, VASP refiner for example.
* [`examples/`](examples/): Examples to use dflow_refiner library.
  * [`miniAutoSteper/`](examples/miniAutoSteper): Simplified version of [`AutoSteper`](https://github.com/Franklalalala/AutoSteper)
  * [`conformation_search_test/`](examples/conformation_search_test): Automated conformation search with `dflow_refiner`
* [`unit_test/`](unit_test/): unit test for some key components.
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
