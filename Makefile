SHELL = /bin/bash

.PHONY: help
help:
	@echo "Commands:"
# 	@echo "venv             : creates .venv"
# 	@echo "install-docs-env : install dependencies into virtual environment for docs+development."
	@echo "install-dev-env  : install dependencies into virtual environment for development."
	@echo "compile-binder   : update the binder environment requirements in .binder/requirements.txt."
# 	@echo "install-prod-env : install dependenceis info virtual environment for non-development."
# 	@echo "compile-docs-env : update the docs environment requirements after changes to related *.in files."
# 	@echo "compile-dev-env  : update the dev environment requirements after changes to related *.in files."
# 	@echo "compile-prod-env : update prod environment requirements after changes to related *.in files."
# 	@echo "compile-all      : update all requirement files prod requirements after changes to related *.in files."
# 	@echo "update-docs-env  : pip install new docs requriements into the environment."
# 	@echo "update-dev-env   : pip install new dev requriements into the environment."
# 	@echo "update-prod-env  : pip install new prod requriements into the environment."
	@echo "docs             : create documentation."
	@echo "serve-docs       : serve documentation."
	@echo "test             : run pytests."
	@echo "tarballs         : create tarballs of data/raw and data/preprocessed for storage on huggingface datasets https://huggingface.co/datasets/Bingpot/bundestag/."

# create a virtual environment
.PHONY: venv
venv:
	uv venv
# 	python3 -m venv .venv
# 	source .venv/bin/activate && \
# 	python3 -m pip install pip==23.0.1 setuptools==67.6.1 wheel==0.40.0 && \
# 	pip install pip-tools==6.12.3

# ==============================================================================
# install requirements
# ==============================================================================
data-in := .config/data.in
data-req := .config/data-requirements.txt

gui-in := .config/gui.in
gui-req := .config/gui-requirements.txt

ml-in := .config/ml.in
ml-req := .config/ml-requirements.txt

style-and-test-in := .config/style-and-test.in
style-and-test-req := .config/style-and-test-requirements.txt

docs-in := .config/docs.in
docs-req := .config/docs-requirements.txt

database-in := .config/database.in
database-req := .config/database-requirements.txt

binder-in := .config/binder.in
binder-req := .config/binder-requirements.txt

docs-env-req := $(docs-req) $(gui-req) $(style-and-test-req) $(ml-req) $(data-req)
dev-env-req := $(ml-req) $(gui-req) $(style-and-test-req) $(data-req)
prod-env-req := $(ml-req) $(data-req)

# environment to generate documentation and development
.PHONY: install-docs-env
install-docs-env: venv
	source .venv/bin/activate && \
	pip-sync $(docs-env-req) && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

# environment for development
.PHONY: install-dev-env
install-dev-env:
	uv sync --group ml --group gui --group style_and_test --group data && \
	uv run python3 -m spacy download de_core_news_sm && \
	pre-commit install
# 	source .venv/bin/activate && \
# 	pip-sync $(dev-env-req) && \
# 	pip install -e . && \
# 	python3 -m spacy download de_core_news_sm && \
# 	pre-commit install

# environment for production
.PHONY: install-prod-env
install-prod-env: venv
	source .venv/bin/activate && \
	pip-sync $(prod-env-req) && \
	pip install -e . && \
	python3 -m spacy download de_core_news_sm && \
	pre-commit install

# ==============================================================================
# compile requirements
# ==============================================================================

# .PHONY: compile-docs-env
# compile-docs-env: compile-docs compile-gui compile-style-and-test compile-ml compile-data

# .PHONY: compile-dev-env
# compile-dev-env: compile-gui compile-style-and-tests compile-ml compile-data

# .PHONY: compile-prod-env
# compile-prod-env: compile-ml compile-data

# .PHONY: compile-all
# compile-all: compile-data compile-gui compile-ml compile-style-and-test compile-docs compile-database compile-binder

# pip-compile .config/mysql.in -o .config/mysql-requirements.txt --resolver=backtracking
# .PHONY: compile-data
# compile-data:
# 	source .venv/bin/activate && \
# 	pip-compile $(data-in) -o $(data-req) --resolver=backtracking

# .PHONY: compile-gui
# compile-gui:
# 	source .venv/bin/activate && \
# 	pip-compile $(gui-in) -o $(gui-req) --resolver=backtracking

# .PHONY: compile-ml
# compile-ml:
# 	source .venv/bin/activate && \
# 	pip-compile $(ml-in) -o $(ml-req) --resolver=backtracking

# .PHONY: compile-style-and-test
# compile-style-and-test:
# 	source .venv/bin/activate && \
# 	pip-compile $(style-and-test-in) -o $(style-and-test-req) --resolver=backtracking

# .PHONY: compile-docs
# compile-docs:
# 	source .venv/bin/activate && \
# 	pip-compile $(docs-in) -o $(docs-req) --resolver=backtracking

# .PHONY: compile-database
# compile-database:
# 	source .venv/bin/activate && \
# 	pip-compile $(database-in) -o $(database-req) --resolver=backtracking

# .PHONY: compile-binder
# compile-binder:
# 	source .venv/bin/activate && \
# 	pip-compile $(binder-in) -o $(binder-req) --resolver=backtracking && \
# 	cp $(binder-req) .binder/requirements.txt

.PHONY: compile-binder
compile-binder:
	uv export --group binder --format requirements.txt -o .binder/requirements.txt


# ==============================================================================
# update requirements and virtual env
# ==============================================================================

.PHONY: update-dev-env
update-dev-env:
	uv sync --group ml --group gui --group style_and_test --group data

# environment for docs and development
# .PHONY: update-docs-env
# update-docs-env:
# 	source .venv/bin/activate && \
# 	pip-sync $(docs-env-req) && \
# 	pip install -e . && \
# 	python -m spacy download de_core_news_sm

# environment for development
# .PHONY: update-dev-env
# update-dev-env:
# 	source .venv/bin/activate && \
# 	pip-sync $(dev-env-req) && \
# 	pip install -e . && \
# 	python -m spacy download de_core_news_sm

# environment for production
# .PHONY: update-prod-env
# update-prod-env:
# 	source .venv/bin/activate && \
# 	pip-sync $(prod-env-req) && \
# 	pip install -e . && \
# 	python -m spacy download de_core_news_sm

# ==============================================================================
# make docs
# ==============================================================================
# assumes make install-docs has been run

# # --output-dir docs/images
# .PHONY: docs
# docs:
# 	source .venv/bin/activate && \
# 	MAKEDOCS=true jupyter nbconvert --to notebook --execute docs/analysis-highlights.ipynb && \
# 	jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown docs/analysis-highlights.ipynb  && \
# 	rm docs/analysis-highlights.nbconvert.ipynb && \
# 	jupyter nbconvert --clear-output docs/analysis-highlights.ipynb

.PHONY: docs
docs:
	MAKEDOCS=true uv run jupyter nbconvert --to notebook --execute docs/analysis-highlights.ipynb && \
	uv run jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown docs/analysis-highlights.ipynb  && \
	rm docs/analysis-highlights.nbconvert.ipynb && \
	uv run jupyter nbconvert --clear-output docs/analysis-highlights.ipynb

.PHONY: serve-docs
serve-docs:
	uv run python -m mkdocs serve


.PHONY: test
test:
	uv run pytest -vx tests

.PHONY: tarballs
tarballs:
	cd data && \
	tar -czvf raw.tar.gz raw && \
	tar -czvf preprocessed.tar.gz preprocessed
