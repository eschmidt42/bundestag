SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "venv             : creates .venv"
	@echo "install-dev-env  : install dependencies into virtual environment for development."
	@echo "install-docs-env : install dependencies into virtual environment for docs+development."
	@echo "compile-binder   : update the binder environment requirements in .binder/requirements.txt."
	@echo "update-dev-env   : pip install new dev requriements into the environment."
	@echo "docs             : create documentation."
	@echo "serve-docs       : serve documentation."
	@echo "test             : run pytests."
	@echo "tarballs         : create tarballs of data/raw and data/preprocessed for storage on huggingface datasets https://huggingface.co/datasets/Bingpot/bundestag/."

# create a virtual environment
.PHONY: venv
venv:
	uv venv

# ==============================================================================
# install requirements
# ==============================================================================


# environment to generate documentation and development
.PHONY: install-docs-env
install-docs-env:
	uv sync --group ml --group gui --group style_and_test --group data --group docs && \
	uv run python3 -m spacy download de_core_news_sm

# environment for development
.PHONY: install-dev-env
install-dev-env:
	uv sync --group ml --group gui --group style_and_test --group data && \
	uv run python3 -m spacy download de_core_news_sm && \
	uv run pre-commit install

.PHONY: install-ci-env
install-ci-env:
	uv sync --group ml --group gui --group style_and_test --group data && \
	uv run python3 -m spacy download de_core_news_sm


# ==============================================================================
# compile requirements
# ==============================================================================

.PHONY: compile-binder
compile-binder:
	uv export --group binder --format requirements.txt -o .binder/requirements.txt


# ==============================================================================
# update requirements and virtual env
# ==============================================================================

.PHONY: update-dev-env
update-dev-env:
	uv sync --group ml --group gui --group style_and_test --group database --group docs && \
	uv run spacy download de_core_news_sm


# ==============================================================================
# make docs
# ==============================================================================

.PHONY: docs
docs:
	MAKEDOCS=true uv run jupyter nbconvert --to notebook --execute docs/fraktionszwang.ipynb && \
	uv run jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown docs/fraktionszwang.ipynb  && \
	rm docs/fraktionszwang.nbconvert.ipynb && \
	uv run jupyter nbconvert --clear-output docs/fraktionszwang.ipynb && \
	MAKEDOCS=true uv run jupyter nbconvert --to notebook --execute docs/analysis-highlights.ipynb && \
	uv run jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown docs/analysis-highlights.ipynb  && \
	rm docs/analysis-highlights.nbconvert.ipynb && \
	uv run jupyter nbconvert --clear-output docs/analysis-highlights.ipynb

.PHONY: serve-docs
serve-docs:
	uv run python -m mkdocs serve

# ==============================================================================
# make test
# ==============================================================================

.PHONY: test
test:
	uv run pytest tests


# ==============================================================================
# make balls
# ==============================================================================

.PHONY: tarballs
tarballs:
	cd data && \
	tar -czvf raw.tar.gz raw && \
	tar -czvf preprocessed.tar.gz preprocessed
