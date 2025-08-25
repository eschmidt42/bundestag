"""
Module for collecting and storing XLSX URIs from Bundestag data sources.
This module provides a command-line interface for fetching XLSX file URIs
from Bundestag websites and storing them in a JSON file for later use.

Functions:
    run: Main command to collect XLSX URIs with configurable parameters
    _setup_logging_callback: Sets up logging configuration for CLI execution

The module uses typer for CLI interface and stores results in JSON format.
"""

import logging
from pathlib import Path

import typer

from bundestag.data.download.bundestag_sheets import (
    create_xlsx_uris_dict,
    store_xlsx_uris,
)
from bundestag.fine_logging import setup_logging

logger = logging.getLogger(__name__)
app = typer.Typer()


@app.command()
def run(
    max_pages: int = 20, json_path: Path = Path("data/raw/bundestag/xlsx_uris.json")
):
    logger.info("Starting xlsx uris collection")
    assert json_path.parent.exists()

    xlsx_uris = create_xlsx_uris_dict(max_pages=max_pages)

    store_xlsx_uris(json_path, xlsx_uris)
    logger.info("Finished xlsx uris collection")


@app.callback(invoke_without_command=True)
def _setup_logging_callback():
    # configure logging for the CLI run
    setup_logging(logging.INFO)


if __name__ == "__main__":
    app()
