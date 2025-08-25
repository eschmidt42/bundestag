import logging
from enum import StrEnum, auto

import typer

import bundestag.paths as paths
from bundestag.data.download.abgeordnetenwatch.download import (
    run as download_abgeordnetenwatch,
)
from bundestag.data.download.bundestag_sheets import run as download_bundestag_sheets
from bundestag.data.download.huggingface import run as download_huggingface
from bundestag.data.transform.abgeordnetenwatch.transform import (
    run as transform_abgeordnetenwatch,
)
from bundestag.data.transform.bundestag_sheets import run as transform_bundestag_sheets
from bundestag.data.utils import RE_SHEET
from bundestag.fine_logging import setup_logging

logger = logging.getLogger(__name__)

app = typer.Typer()


class Sources(StrEnum):
    abgeordnetenwatch = auto()
    bundestag_sheet = auto()
    huggingface = auto()


sources_values = [k.value for k in Sources]


@app.command(help="Download data from a chosen source")
def download(
    source: str = typer.Argument(
        ...,
        help=f"One of {sources_values}",
    ),
    legislature_id: int = typer.Argument(
        111,
        help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
    ),
    dry: bool = typer.Option(False, help="Dry or not"),
    data_path: str = typer.Option("data", help="Root dir for data storage"),
    max_mandates: int = typer.Option(
        999,
        help="Max number of mandates to download (abgeordnetenwatch specific)",
    ),
    max_polls: int = typer.Option(
        999,
        help="Max number of polls to download (abgeordnetenwatch specific)",
    ),
    nmax: int = typer.Option(
        999, help="Max number of sheets to download (bundestag_sheet specific)"
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
    logger.info(f"Downloading from {source}")

    _paths = paths.get_paths(data_path)

    match source:
        case Sources.abgeordnetenwatch:
            download_abgeordnetenwatch(
                legislature_id=legislature_id,
                dry=dry,
                raw_path=_paths.raw_abgeordnetenwatch,
                max_mandates=max_mandates,
                max_polls=max_polls,
                assume_yes=y,
            )

        case Sources.bundestag_sheet:
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

        case Sources.huggingface:
            download_huggingface(
                path=_paths.root_path,
                dry=dry,
                assume_yes=y,
            )


@app.command(help="Transform data for a chosen source")
def transform(
    source: str = typer.Argument(
        Sources.abgeordnetenwatch.value, help=f"One of {sources_values}"
    ),
    legislature_id: int = typer.Argument(
        111,
        help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
    ),
    dry: bool = typer.Argument(False, help="Dry or not"),
    data_path: str = typer.Argument("data", help="Root dir for data storage"),
):
    logger.info(f"Processing {source}")

    _paths = paths.get_paths(data_path)

    match source:
        case Sources.abgeordnetenwatch:
            logger.info(f"for legislature id {legislature_id}")

            transform_abgeordnetenwatch(
                legislature_id=legislature_id,
                raw_path=_paths.raw_abgeordnetenwatch,
                preprocessed_path=_paths.preprocessed_abgeordnetenwatch,
                dry=dry,
            )

        case Sources.bundestag_sheet:
            transform_bundestag_sheets(
                html_dir=_paths.raw_bundestag_html,
                sheet_dir=_paths.raw_bundestag_sheets,
                preprocessed_path=_paths.preprocessed_bundestag,
                dry=dry,
            )

        case _:
            raise ValueError(f"{source=} unexpected")


@app.callback(invoke_without_command=True)
def _setup_logging_callback():
    # configure logging for the CLI run
    setup_logging(logging.INFO)


if __name__ == "__main__":
    app()
