"""
# Download CLI

This module provides CLI commands to download data from various sources.
"""

import logging

import typer

import bundestag.paths as paths
from bundestag.cli.utils import (
    ARGUMENT_LEGISLATURE_ID,
    OPTION_DATA_PATH,
    OPTION_DRY,
    OPTION_Y,
)
from bundestag.data.download.abgeordnetenwatch.download import EntityEnum
from bundestag.data.download.abgeordnetenwatch.download import (
    run as download_abgeordnetenwatch,
)
from bundestag.data.download.bundestag_sheets import run as download_bundestag_sheets
from bundestag.data.download.huggingface import run as download_huggingface
from bundestag.data.utils import RE_SHEET

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command(help="Download data from the abgeordnetenwatch API.")
def abgeordnetenwatch(
    legislature_id: int = ARGUMENT_LEGISLATURE_ID,
    dry: bool = OPTION_DRY,
    data_path: str = OPTION_DATA_PATH,
    entity: EntityEnum = typer.Option(
        EntityEnum.all,
        help="Entity to request.",
    ),
    max_mandates: int = typer.Option(
        999,
        help="Max number of mandates to download (abgeordnetenwatch specific)",
    ),
    max_polls: int = typer.Option(
        999,
        help="Max number of polls to download (abgeordnetenwatch specific)",
    ),
    y: bool = OPTION_Y,
    timeout: float = typer.Option(
        42.0, help="Timeout in seconds to use with httpx get requests."
    ),
):
    """Download data from the abgeordnetenwatch API.

    Args:
        legislature_id (int): The ID of the legislature to download data for. Defaults to 111.
        dry (bool, optional): If `True`, don't actually download anything. Defaults to False.
        data_path (str, optional): The path to the data directory. Defaults to "data".
        entity (EntityEnum, optional): The entity to request from the API. Defaults to EntityEnum.all.
        max_mandates (int, optional): Maximum number of mandates to download. Defaults to 999.
        max_polls (int, optional): Maximum number of polls to download. Defaults to 999.
        y (bool, optional): Assume yes to all prompts. Defaults to False.
        timeout (float, optional): Timeout in seconds for HTTP requests. Defaults to 42.0.

    Examples:
        To download all data for legislature 161:
        `bundestag download abgeordnetenwatch 161`
    """
    _paths = paths.get_paths(data_path)

    download_abgeordnetenwatch(
        legislature_id=legislature_id,
        dry=dry,
        raw_path=_paths.raw_abgeordnetenwatch,
        max_mandates=max_mandates,
        max_polls=max_polls,
        assume_yes=y,
        entity=entity,
        timeout=timeout,
    )


@app.command(help="Download data from the bundestag.")
def bundestag(
    dry: bool = OPTION_DRY,
    data_path: str = OPTION_DATA_PATH,
    nmax: int = typer.Option(
        None, help="Max number of sheets to download (bundestag_sheet specific)"
    ),
    y: bool = OPTION_Y,
    do_create_xlsx_uris_json: bool = typer.Option(
        False,
        help="bundestag_sheet specific parameter. If passed then a new xlsx_uris.json will be created from https://www.bundestag.de/parlament/plenum/abstimmung/liste.",
    ),
    max_pages: int = typer.Option(
        20,
        help="bundestag_sheet specific parameter. Max number of pages to flip though on https://www.bundestag.de/parlament/plenum/abstimmung/liste/ to create uris_xlsx.json",
    ),
):
    """Download data from the bundestag.

    Args:
        dry (bool, optional): If `True`, don't actually download anything. Defaults to False.
        data_path (str, optional): The path to the data directory. Defaults to "data".
        nmax (int, optional): Max number of sheets to download. Defaults to None.
        y (bool, optional): Assume yes to all prompts. Defaults to False.
        do_create_xlsx_uris_json (bool, optional): If `True`, a new `xlsx_uris.json` will be created. Defaults to False.
        max_pages (int, optional): Max number of pages to search for Excel file URIs. Defaults to 20.

    Examples:
        To recreate the Excel file URIs list by searching up to 50 pages:
        `bundestag download bundestag --do-create-xlsx-uris-json`
    """
    _paths = paths.get_paths(data_path)

    download_bundestag_sheets(
        html_dir=_paths.raw_bundestag_html,
        sheet_dir=_paths.raw_bundestag_sheets,
        nmax=nmax,
        dry=dry,
        pattern=RE_SHEET,
        assume_yes=y,
        do_create_xlsx_uris_json=do_create_xlsx_uris_json,
        max_pages=max_pages,
    )


@app.command(help="Download data from huggingface.")
def huggingface(
    dry: bool = OPTION_DRY,
    data_path: str = OPTION_DATA_PATH,
    y: bool = OPTION_Y,
):
    """Download data from huggingface.

    Args:
        dry (bool, optional): If `True`, don't actually download anything. Defaults to False.
        data_path (str, optional): The path to the data directory. Defaults to "data".
        y (bool, optional): Assume yes to all prompts. Defaults to False.

    Examples:
        To download the data from Hugging Face:
        `bundestag download huggingface`
    """
    _paths = paths.get_paths(data_path)

    download_huggingface(
        path=_paths.root_path,
        dry=dry,
        assume_yes=y,
    )
