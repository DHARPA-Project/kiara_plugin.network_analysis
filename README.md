[![PyPI status](https://img.shields.io/pypi/status/kiara_plugin.network_analysis.svg)](https://pypi.python.org/pypi/kiara_plugin.network_analysis/)
[![PyPI version](https://img.shields.io/pypi/v/kiara_plugin.network_analysis.svg)](https://pypi.python.org/pypi/kiara_plugin.network_analysis/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/kiara_plugin.network_analysis.svg)](https://pypi.python.org/pypi/kiara_plugin.network_analysis/)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FDHARPA-Project%2Fkiara%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/DHARPA-Project/kiara_plugin.network_analysis/goto?ref=develop)
[![Coverage Status](https://coveralls.io/repos/github/DHARPA-Project/kiara_plugin.network_analysis/badge.svg?branch=develop)](https://coveralls.io/github/DHARPA-Project/kiara_plugin.network_analysis?branch=develop)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# [**kiara**](https://dharpa.org/kiara.documentation) plugin: (network_analysis)

Network-related data-types, modules and pipelines for kiara.

 - Documentation: [https://DHARPA-Project.github.io/kiara_plugin.network_analysis](https://DHARPA-Project.github.io/kiara_plugin.network_analysis)
 - Code: [https://github.com/DHARPA-Project/kiara_plugin.network_analysis](https://github.com/DHARPA-Project/kiara_plugin.network_analysis)
 - `kiara`: [https://docs.dharpa.org](https://docs.dharpa.org)

## Description

TODO

## Development

### Requirements

- uv ( https://docs.astral.sh/uv/ )
- git
- make (on Linux / Mac OS X -- optional)

### Check out the source code & enter the project directory

```
git clone https://github.com/DHARPA-Project/kiara_plugin.network_analysis
cd kiara_plugin.network_analysis
```

### Prepare development environment

The recommended way to setup a development environment is to use [uv](https://docs.astral.sh/uv/). Check out [their install instructions](https://docs.astral.sh/uv/getting-started/installation/).

Once you have `uv` installed, you can either run `kiara` using the `uv run` command:

```
uv run kiara module list
```

or, activate the virtual environment and run `kiara` directly:

```
uv sync  # to make sure the virtualenv exists (and is up to date)
source .venv/bin/activate
kiara module list
```

### Run one of the included notebooks

For any of the [marimo](https://marimo.io/) under `docs/notebooks/marimo`, you can run them with:

```
uv run marimo edit docs/notebooks/marimo/getting_started.py
```

Likewise, for the [jupyter](https://jupyter.org/) notebooks you can run them with:
```
uv run jupyter lab docs/notebooks/jupyter/getting_started.ipynb
```

### Running pre-defined development-related tasks

The included `Makefile` file includes some useful tasks that help with development. This requires `uv` and the `make` tool to be
installed, which should be the case for Linux & Mac OS X systems.

- `make test`: runs the unit tests
- `make mypy`: run mypy checks
- `make lint`: run the `ruff` linter on the source code
- `make format`: run the `ruff` formatter on the source code (similar to `black`)
- `make docs`: build the documentation  (into `build` folder)
- `make docs-serve`: serve the documentation (on port 8000)

Alternatively, if you don't have the `make` command available, you can use `uv` directly to run those tasks:

- `uv run pytest tests`
- `uv run mypy src/`
- `uv run ruff check --fix src/`
- `uv run ruff format src/`

## Copyright & license

This project is MPL v2.0 licensed, for the license text please check the [LICENSE](/LICENSE) file in this repository.
