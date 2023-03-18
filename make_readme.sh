#!/bin/bash
# https://jackmckew.dev/make-a-readme-documentation-with-jupyter-notebooks.html
jupyter nbconvert --ClearMetadataPreprocessor.enabled=True --ClearOutput.enabled=True --to markdown README.ipynb
cp README.md index.md
