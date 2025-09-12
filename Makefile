SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
	@echo "install-dev-env  : install dependencies into virtual environment for development."
	@echo "update-dev-env   : pip install new dev requriements into the environment."
	@echo "install-docs-env : install dependencies into virtual environment for docs+development."
	@echo "docs             : create documentation."
	@echo "serve-docs       : serve documentation."
	@echo "compile-binder   : update the binder environment requirements in .binder/requirements.txt."
	@echo "test             : run pytests."
	@echo "tarballs         : create tarballs of data/raw and data/preprocessed for storage on huggingface datasets https://huggingface.co/datasets/Bingpot/bundestag/."
	@echo "coverage         : compute test coverage"

# ==============================================================================
# dev
# ==============================================================================

.PHONY: install-dev-env
install-dev-env:
	uv sync --all-extras --group dev --group tests --group docs && \
	uv run python3 -m spacy download de_core_news_sm && \
	uv run pre-commit install

.PHONY: update-dev-env
update-dev-env:
	uv sync --all-extras --group dev --group tests --group docs && \
	uv run spacy download de_core_news_sm

# ==============================================================================
# docs
# ==============================================================================

.PHONY: install-docs-env
install-docs-env:
	uv sync --group binder --group docs && \
	uv run python3 -m spacy download de_core_news_sm


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
# ci tests
# ==============================================================================

.PHONY: install-ci-env
install-ci-env:
	uv sync --all-extras --group tests && \
	uv run python3 -m spacy download de_core_news_sm

# ==============================================================================
# binder
# ==============================================================================

.PHONY: compile-binder
compile-binder:
	uv export --group binder --format requirements.txt -o .binder/requirements.txt

# ==============================================================================
# test
# ==============================================================================

.PHONY: test
test:
	uv run pytest tests

# ==============================================================================
# coverage
# ==============================================================================

.PHONY: coverage
coverage:
	uv run pytest --cov=src --cov-report html  tests

# ==============================================================================
# make balls for huggingface uploads / downloads
# ==============================================================================

.PHONY: tarballs
tarballs:
	cd data && \
	tar -czvf raw.tar.gz raw && \
	tar -czvf preprocessed.tar.gz preprocessed
