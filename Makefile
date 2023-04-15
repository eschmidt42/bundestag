SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "venv         : creates .venv"
	@echo "install-docs : install dependencies into virtual environment for docs+development."
	@echo "install-dev  : install dependencies into virtual environment for development."
	@echo "install      : install dependenceis info virtual environment for non-development."
	@echo "compile-all  : update all requirement files after changes to pyproject.toml core/dev/docs dependencies."
	@echo "compile-docs : update the environment docs requirements after changes to pyproject.toml docs dependencies."
	@echo "compile-dev  : update the environment dev requirements after changes to pyproject.toml dev dependencies."
	@echo "compile      : update the environment non-dev requirements after changes to pyproject.toml dependencies."
	@echo "update-docs  : pip install new docs requriements into the environment."
	@echo "update-dev   : pip install new dev requriements into the environment."
	@echo "update       : pip install new requriements into the environment."
	@echo "docs         : create documentation."
	@echo "serve-docs   : serve documentation."
	@echo "test         : run pytests."
	@echo "tarballs     : create tarballs of data/raw and data/preprocessed for storage on huggingface datasets https://huggingface.co/datasets/Bingpot/bundestag/."

.PHONY: venv
venv:
	python3 -m venv .venv
	source .venv/bin/activate && \
	python3 -m pip install pip==23.0.1 setuptools==67.6.1 wheel==0.40.0 && \
	pip install pip-tools==6.12.3


# setup environment
.PHONY: install-docs
install-docs: venv
	source .venv/bin/activate && \
	pip-sync requirements/docs-requirements.txt && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

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

.PHONY: compile-all
compile-all:
	source .venv/bin/activate && \
	pip-compile .config/core.in -o .config/core-requirements.txt --resolver=backtracking && \
	pip-compile .config/dev.in  -o .config/dev-requirements.txt  --resolver=backtracking && \
	cp .config/dev-requirements.txt .binder/requirements.txt && \
	pip-compile .config/docs.in -o .config/docs-requirements.txt --resolver=backtracking


.PHONY: compile-docs
compile-docs:
	source .venv/bin/activate && \
	pip-compile .config/docs.in -o .config/docs-requirements.txt --resolver=backtracking

.PHONY: compile-dev
compile-dev:
	source .venv/bin/activate && \
	pip-compile .config/dev.in -o .config/dev-requirements.txt   --resolver=backtracking && \
	cp .config/dev-requirements.txt .binder/requirements.txt

.PHONY: compile
compile:
	source .venv/bin/activate && \
	pip-compile .config/core.in -o .config/core-requirements.txt  --resolver=backtracking


# update requirements and virtual env after changed to pyproject.toml
.PHONY: update-docs
update-docs:
	source .venv/bin/activate && \
	pip-sync .config/docs-requirements.txt .config/dev-requirements.txt .config/core-requirements.txt && \
	pip install -e . && \
	python -m spacy download de_core_news_sm

.PHONY: update-dev
update-dev:
	source .venv/bin/activate && \
	pip-sync .config/dev-requirements.txt .config/core-requirements.txt && \
	pip install -e . && \
	python -m spacy download de_core_news_sm

.PHONY: update
update:
	source .venv/bin/activate && \
	pip-sync .config/core-requirements.txt && \
	pip install -e . && \
	python -m spacy download de_core_news_sm

# make docs
# # --output-dir docs/images
.PHONY: docs
docs:
	source .venv/bin/activate && \
	MAKEDOCS=true jupyter nbconvert --to notebook --execute docs/analysis-highlights.ipynb && \
	jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown docs/analysis-highlights.ipynb  && \
	rm docs/analysis-highlights.nbconvert.ipynb && \
	jupyter nbconvert --clear-output docs/analysis-highlights.ipynb

# run tests
.PHONY: test
test:
	source .venv/bin/activate && \
	pytest -vx --ignore src/legacy .

.PHONY: tarballs
tarballs:
	source .venv/bin/activate && \
	cd data && \
	tar -czvf raw.tar.gz raw && \
	tar -czvf preprocessed.tar.gz preprocessed

.PHONY: serve-docs
serve-docs:
	source .venv/bin/activate && \
	python3 -m mkdocs serve
