import typer

import bundestag.data.download.abgeordnetenwatch.download as download_aw
import bundestag.data.download.bundestag_sheets as download_bs
import bundestag.data.download.huggingface as download_hf
import bundestag.data.transform.abgeordnetenwatch.transform as transform_aw
import bundestag.data.transform.bundestag_sheets as transform_bs
import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.paths as paths

logger = logging.logger

app = typer.Typer()

VALID_SOURCES = ["abgeordnetenwatch", "bundestag_sheet", "huggingface"]


@app.command(help="Download data from a chosen source")
def download(
    source: str = typer.Argument(
        ...,
        help=f"One of {VALID_SOURCES}",
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
):
    logger.info(f"Downloading from {source}")

    _paths = paths.get_paths(data_path)

    if source == VALID_SOURCES[0]:
        logger.info(f"for legislature id {legislature_id}")

        # run steps for abgeordetenwatch
        download_aw.run(
            legislature_id=legislature_id,
            dry=dry,
            raw_path=_paths.raw_abgeordnetenwatch,
            max_mandates=max_mandates,
            max_polls=max_polls,
            assume_yes=y,
        )

    elif source == VALID_SOURCES[1]:
        # run steps for bundestag sheets
        download_bs.run(
            html_dir=_paths.raw_bundestag_html,
            sheet_dir=_paths.raw_bundestag_sheets,
            nmax=nmax,
            dry=dry,
            pattern=data_utils.RE_SHEET,
            assume_yes=y,
        )

    elif source == VALID_SOURCES[2]:
        # run steps for huggingface
        download_hf.run(
            path=_paths.root_path,
            dry=dry,
            assume_yes=y,
        )

    else:
        raise ValueError(f"{source=} unexpected")


@app.command(help="Transform data for a chosen source")
def transform(
    source: str = typer.Argument(VALID_SOURCES[0], help=f"One of {VALID_SOURCES}"),
    legislature_id: int = typer.Argument(
        111,
        help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
    ),
    dry: bool = typer.Argument(False, help="Dry or not"),
    data_path: str = typer.Argument("data", help="Root dir for data storage"),
):
    logger.info(f"Processing {source}")

    _paths = paths.get_paths(data_path)

    if source == VALID_SOURCES[0]:
        logger.info(f"for legislature id {legislature_id}")

        # run steps for abgeordetenwatch
        transform_aw.run(
            legislature_id=legislature_id,
            raw_path=_paths.raw_abgeordnetenwatch,
            preprocessed_path=_paths.preprocessed_abgeordnetenwatch,
            dry=dry,
        )

    elif source == VALID_SOURCES[1]:
        # run steps for bundestag sheets
        transform_bs.run(
            html_path=_paths.raw_bundestag_html,
            sheet_path=_paths.raw_bundestag_sheets,
            preprocessed_path=_paths.preprocessed_bundestag,
            dry=dry,
        )

    else:
        raise ValueError(f"{source=} unexpected")


if __name__ == "__main__":
    app()
