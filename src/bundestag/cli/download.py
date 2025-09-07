import logging

import typer

import bundestag.paths as paths
from bundestag.cli.utils import OPTION_DRY
from bundestag.data.download.abgeordnetenwatch.download import (
    run as download_abgeordnetenwatch,
)
from bundestag.data.download.bundestag_sheets import run as download_bundestag_sheets
from bundestag.data.download.huggingface import run as download_huggingface
from bundestag.data.utils import RE_SHEET

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command(help="Download data from abgeordnetenwatch.")
def abgeordnetenwatch(
    legislature_id: int = typer.Argument(
        111,
        help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
    ),
    dry: bool = OPTION_DRY,
    data_path: str = typer.Option("data", help="Root dir for data storage"),
    max_mandates: int = typer.Option(
        999,
        help="Max number of mandates to download (abgeordnetenwatch specific)",
    ),
    max_polls: int = typer.Option(
        999,
        help="Max number of polls to download (abgeordnetenwatch specific)",
    ),
    y: bool = typer.Option(
        default=False,
        help="Assume yes to all prompts and run non-interactively.",
    ),
):
    _paths = paths.get_paths(data_path)

    download_abgeordnetenwatch(
        legislature_id=legislature_id,
        dry=dry,
        raw_path=_paths.raw_abgeordnetenwatch,
        max_mandates=max_mandates,
        max_polls=max_polls,
        assume_yes=y,
    )


@app.command(help="Download data from the bundestag.")
def bundestag(
    dry: bool = OPTION_DRY,
    data_path: str = typer.Option("data", help="Root dir for data storage"),
    nmax: int = typer.Option(
        None, help="Max number of sheets to download (bundestag_sheet specific)"
    ),
    y: bool = typer.Option(
        default=False,
        help="Assume yes to all prompts and run non-interactively.",
    ),
    do_create_xlsx_uris_json: bool = typer.Option(
        False,
        help="bundestag_sheet specific parameter. If passed then a new xlsx_uris.json will be created from https://www.bundestag.de/parlament/plenum/abstimmung/liste.",
    ),
    max_pages: int = typer.Option(
        20,
        help="bundestag_sheet specific parameter. Max number of pages to flip though on https://www.bundestag.de/parlament/plenum/abstimmung/liste/ to create uris_xlsx.json",
    ),
):
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
    data_path: str = typer.Option("data", help="Root dir for data storage"),
    y: bool = typer.Option(
        default=False,
        help="Assume yes to all prompts and run non-interactively.",
    ),
):
    _paths = paths.get_paths(data_path)

    download_huggingface(
        path=_paths.root_path,
        dry=dry,
        assume_yes=y,
    )
