from pathlib import Path

import pandas as pd

import bundestag.data.download.bundestag_sheets as download_sheets
import bundestag.data.transform.bundestag_sheets as transform_sheets
import bundestag.data.utils as data_utils
import bundestag.logging as logging

logger = logging.logger


def get_multiple_sheets(
    html_path: Path,
    sheet_path: Path,
    nmax: int | None = None,
    dry: bool = False,
    validate: bool = False,
) -> pd.DataFrame:
    "Downloading, storing, excel file detection and processing of votes in bundestag sheets to pd.DataFrame"

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = data_utils.get_file_paths(html_path, pattern=data_utils.RE_HTM)
    # extract excel sheet uris from htm files
    sheet_uris = download_sheets.collect_sheet_uris(html_file_paths)
    # download excel files
    download_sheets.download_multiple_sheets(
        sheet_uris, sheet_dir=sheet_path, nmax=nmax, dry=dry
    )
    # locate downloaded excel files
    sheet_files = data_utils.get_file_paths(sheet_path, pattern=data_utils.RE_FNAME)
    # process excel files
    file_title_maps = transform_sheets.get_file2poll_maps(sheet_uris, sheet_path)
    df = transform_sheets.get_multiple_sheets_df(
        sheet_files, file_title_maps=file_title_maps, validate=validate
    )
    return df
