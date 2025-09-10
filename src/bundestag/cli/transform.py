"""
# Transform CLI

This module provides CLI commands to transform downloaded data.
"""

import logging

import typer

import bundestag.paths as paths
from bundestag.cli.utils import ARGUMENT_LEGISLATURE_ID, OPTION_DATA_PATH, OPTION_DRY
from bundestag.data.transform.abgeordnetenwatch.transform import (
    run as _transform_abgeordnetenwatch,
)
from bundestag.data.transform.bundestag_sheets import Source as SheetsSource
from bundestag.data.transform.bundestag_sheets import run as _transform_bundestag_sheets

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command(help="Transform bundestag sheet data.")
def bundestag_sheets(
    dry: bool = OPTION_DRY,
    data_path: str = OPTION_DATA_PATH,
    sheet_source: SheetsSource = typer.Option(
        SheetsSource.json_file.value,
        help=f"bundestag_sheet specific parameter. Switch between xlsx uri sources. Options: {[k.value for k in SheetsSource]}",
    ),
):
    """Transform bundestag sheet data.

    Args:
        dry (bool, optional): If `True`, don't actually perform the transformation. Defaults to False.
        data_path (str, optional): The path to the data directory. Defaults to "data".
        sheet_source (SheetsSource, optional): The source for sheet URIs. Defaults to "json_file".

    Examples:
        To transform the data using the default JSON file source:
        `bundestag transform bundestag-sheets`
    """
    _paths = paths.get_paths(data_path)

    _transform_bundestag_sheets(
        html_dir=_paths.raw_bundestag_html,
        sheet_dir=_paths.raw_bundestag_sheets,
        preprocessed_path=_paths.preprocessed_bundestag,
        dry=dry,
        source=sheet_source,
    )


@app.command(help="Transform abgeordnetenwatch data.")
def abgeordnetenwatch_data(
    legislature_id: int = ARGUMENT_LEGISLATURE_ID,
    dry: bool = OPTION_DRY,
    data_path: str = OPTION_DATA_PATH,
):
    """Transform abgeordnetenwatch data.

    Args:
        legislature_id (int): The ID of the legislature to transform data for. Defaults to 111.
        dry (bool, optional): If `True`, don't actually perform the transformation. Defaults to False.
        data_path (str, optional): The path to the data directory. Defaults to "data".

    Examples:
        To transform data for legislature 161:
        `bundestag transform abgeordnetenwatch-data 161`
    """
    _paths = paths.get_paths(data_path)

    _transform_abgeordnetenwatch(
        legislature_id=legislature_id,
        raw_path=_paths.raw_abgeordnetenwatch,
        preprocessed_path=_paths.preprocessed_abgeordnetenwatch,
        dry=dry,
    )
