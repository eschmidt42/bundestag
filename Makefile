SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "venv         : creates .venv"
	@echo "install-dev  : install dependencies into virtual environment for development."
	@echo "install      : install dependenceis info virtual environment for non-development."
	@echo "compile-dev  : update the environment dev requirements after changes to pyproject.toml dev dependencies."
	@echo "compile      : update the environment non-dev requirements after changes to pyproject.toml dependencies."
	@echo "update-dev   : pip install new dev requriements into the environment."
	@echo "update       : pip install new requriements into the environment."
	@echo "docs         : create documentation."
	@echo "test         : run pytests."
	@echo "tarballs     : create tarballs of data/raw and data/preprocessed for storage on huggingface datasets https://huggingface.co/datasets/Bingpot/bundestag/."

.PHONY: venv
venv:
	python3 -m venv .venv
	source .venv/bin/activate && \
	python3 -m pip install pip setuptools wheel && \
	pip install pip-tools==6.12.3


# setup environment
.PHONY: install-dev
install-dev: venv
	source .venv/bin/activate && \
	pip-sync requirements/dev-requirements.txt && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

.PHONY: install
install: venv
	source .venv/bin/activate && \
	pip-sync requirements/requirements.txt && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

.PHONY: compile-dev
compile-dev:
	source .venv/bin/activate && \
	pip-compile --extra dev -o requirements/dev-requirements.txt pyproject.toml  --resolver=backtracking

.PHONY: compile
compile:
	source .venv/bin/activate && \
	pip-compile --extra dev -o requirements/dev-requirements.txt pyproject.toml  --resolver=backtracking



# update requirements and virtual env after changed to pyproject.toml
.PHONY: update-dev
update-dev:
	source .venv/bin/activate && \
	pip-sync requirements/dev-requirements.txt && \
	pip install -e . && \
	python -m spacy download de_core_news_sm

.PHONY: update
update:
	source .venv/bin/activate && \
	pip-sync requirements/requirements.txt && \
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

.PHONY: tarballs
tarballs:
	source .venv/bin/activate && \
	cd data && \
	tar -czvf raw.tar.gz raw && \
	tar -czvf preprocessed.tar.gz preprocessed
