.PHONY: run_admin_app

install-poetry:
	curl -sSL https://install.python-poetry.org | python3 -

install-homebrew:
	/bin/bash -c "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

install-pyenv:
	@command -v brew >/dev/null 2>&1 || $(MAKE) install-homebrew
	brew install pyenv

install-python-3.8:
	@echo "Python 3.8 not found. Installing Python 3.8 using pyenv."
	@pyenv install 3.8
	@pyenv local 3.8

check-deps:
	@command -v poetry >/dev/null 2>&1 || $(MAKE) install-poetry


shell: check-deps ## Get into a poetry shell
	poetry shell

setup: check-deps shell ## Install dependencies into your poetry environment.
	poetry install

# app
run_admin_app:
	python -m streamlit run app.py

# Installs dependencies for the admin app.
setup_admin_app:
	pip install .

# Linting and formatting below.
run_mypy:  ## Run mypy
	mypy --config-file=mypy.ini .

run_flake8:  ## Run flake8
	flake8 --ignore=E203,E501,W503 --exclude=venv,.venv,pyvenv,tmp,*_pb2.py,*_pb2.pyi,images/*/src .

check_black:  ## Check to see if files would be updated with black.
    # Exclude pyvenv and all generated protobuf code.
	black --check --exclude=".venv|venv|pyvenv|.*_pb2.py|.*_pb2.pyi" .

run_black:  ## Run black to format files.
    # Exclude pyvenv, tmp, and all generated protobuf code.
	black --exclude=".venv|venv|pyvenv|tmp|.*_pb2.py|.*_pb2.pyi" .

check_isort:  ## Check if files would be updated with isort.
	isort --profile black --check --skip=venv --skip=pyvenv --skip=.venv --skip-glob='*_pb2.py*' .

run_isort:  ## Run isort to update imports.
	isort --profile black --skip=pyvenv --skip=venv --skip=tmp --skip=.venv --skip-glob='*_pb2.py*' .


fmt_lint: shell ## lint/fmt in current python environment
	make run_black run_isort run_flake8

# Test below
test: shell ## Run tests.
	python -m pytest -vvs semantic_model_generator

test_github_workflow:  ## For use on github workflow.
	python -m pytest -vvs semantic_model_generator

# Release
update-version: ## Bump poetry and github version. TYPE should be `patch` `minor` or `major`
	@echo "Updating Poetry version ($(TYPE)) and creating a Git tag..."
	@poetry version $(TYPE)
	@echo "Version updated to $$VERSION. Update the CHANGELOG.md `make release`"

release: ## Runs the release workflow.
	@VERSION=$$(poetry version -s) && git commit --allow-empty  -m "Bump version to $$VERSION" && git tag release/v$$VERSION && \
 	git push origin HEAD && git push origin HEAD --tags

build: ## Clean the dist dir and build the whl file
	rm -rf dist
	mkdir dist
	poetry build

help: ## Show this help.
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's