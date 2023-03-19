SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "activate     : activates the virtual environment in .venv."
	@echo "dev-install  : creates a virtual environment for development."
	@echo "dev-update   : update the dev environment after changes to pyproject.toml dependencies."
	@echo "docs         : creates documentation."


# setup environment
.PHONY: dev-install
dev-install:
	python3 -m venv .venv
	source .venv/bin/activate && \
	python3 -m pip install pip setuptools wheel && \
	pip install pip-tools==6.12.3 && \
	pip-sync requirements/dev-requirements.txt && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

# update requirements and virtual env after changed to pyproject.toml
.PHONY: dev-update
dev-update:
	source .venv/bin/activate && \
	pip-compile -o requirements/requirements.txt pyproject.toml --resolver=backtracking && \
	pip-compile --extra dev -o requirements/dev-requirements.txt pyproject.toml  --resolver=backtracking && \
	pip-sync requirements/dev-requirements.txt && \
	pip install -e . && \
	python -m spacy download de_core_news_sm

# make docs
.PHONY: docs
docs:
	source .venv/bin/activate && \
	jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown README.ipynb && \
	cp README.md index.md

# run tests
.PHONY: test
test:
	source .venv/bin/activate && \
	pytest -vx .
