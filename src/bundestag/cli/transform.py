import logging

import typer

import bundestag.paths as paths
from bundestag.cli.utils import OPTION_DRY
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
    data_path: str = typer.Argument("data", help="Root dir for data storage"),
    sheet_source: SheetsSource = typer.Option(
        SheetsSource.json_file.value,
        help=f"bundestag_sheet specific parameter. Switch between xlsx uri sources. Options: {[k.value for k in SheetsSource]}",
    ),
):
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
    legislature_id: int = typer.Argument(
        111,
        help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
    ),
    dry: bool = OPTION_DRY,
    data_path: str = typer.Argument("data", help="Root dir for data storage"),
):
    _paths = paths.get_paths(data_path)

    _transform_abgeordnetenwatch(
        legislature_id=legislature_id,
        raw_path=_paths.raw_abgeordnetenwatch,
        preprocessed_path=_paths.preprocessed_abgeordnetenwatch,
        dry=dry,
    )
