.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)


clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg' -exec rm -rf {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr .mypy_cache

init: clean ## initialize a development environment (to be run in virtualenv)
	git init
	git checkout -b develop || true
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg
	git add "*" ".*"
	uv run pre-commit run --all-files || true
	git add "*" ".*"
	git commit -m 'chore: initial commit'

mypy: ## run mypy
	uv run mypy src/

ruff:
	uv run ruff check --fix src/

test: ## run tests quickly with the default Python
	uv run py.test tests

doc:
	uv run pdoc kiara_plugin.network_analysis

check: ruff mypy test ## run dev-related checks

pre-commit: ## run pre-commit on all files
	uv run pre-commit run --all-files
