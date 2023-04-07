from pathlib import Path

import pandas as pd

import bundestag.data.download.abgeordnetenwatch as download_aw
import bundestag.data.download.bundestag_sheets as download_sheets
import bundestag.data.transform.abgeordnetenwatch as transform_aw
import bundestag.data.transform.bundestag_sheets as transform_sheets
import bundestag.data.utils as data_utils
import bundestag.logging as logging

logger = logging.logger


def get_multiple_sheets(
    html_path: Path, sheet_path: Path, nmax: int = None, dry: bool = False
) -> pd.DataFrame:
    "Convenience function to perform downloading, storing, excel file detection and processing of votes to pd.DataFrame"

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = data_utils.get_file_paths(
        html_path, pattern=transform_sheets.RE_HTM
    )
    # extract excel sheet uris from htm files
    sheet_uris = download_sheets.collect_sheet_uris(html_file_paths)
    # download excel files
    download_sheets.download_multiple_sheets(
        sheet_uris, sheet_path=sheet_path, nmax=nmax, dry=dry
    )
    # locate downloaded excel files
    sheet_files = data_utils.get_file_paths(
        sheet_path, pattern=transform_sheets.RE_FNAME
    )
    # process excel files
    file_title_maps = transform_sheets.get_file2poll_maps(
        sheet_uris, sheet_path
    )
    df = transform_sheets.get_multiple_sheets_df(
        sheet_files, file_title_maps=file_title_maps
    )
    return df


def get_legislature_data(
    legislature_id: int,
    dry: bool = False,
    raw_path: Path = None,
    preprocessed_path: Path = None,
    max_polls: int = 999,
) -> pd.DataFrame:
    if not dry and (raw_path is None or preprocessed_path is None):
        raise ValueError(
            f"When {dry=} `raw_path` and or `preprocessed_path` cannot be None."
        )

    data = download_aw.request_poll_data(
        legislature_id, dry=dry, num_polls=max_polls
    )
    download_aw.store_polls_json(data, legislature_id, dry=dry, path=raw_path)
    df = transform_aw.get_polls_data(legislature_id, path=raw_path)
    if not dry:
        file = preprocessed_path / f"df_polls_{legislature_id}.parquet"
        logger.debug(f"writing to {file}")
        df.to_parquet(path=file)

    data = download_aw.request_mandates_data(
        legislature_id, dry=dry, num_mandates=999
    )
    download_aw.store_mandates_json(
        data, legislature_id, dry=dry, path=raw_path
    )
    df = transform_aw.get_mandates_data(legislature_id, path=raw_path)
    if not dry:
        file = preprocessed_path / f"df_mandates_{legislature_id}.parquet"
        logger.debug(f"Writing to {file}")
        df.to_parquet(path=file)

    transform_aw.get_all_remaining_vote_data(
        legislature_id, dry=dry, path=raw_path
    )
    df_all_votes = transform_aw.compile_votes_data(
        legislature_id, path=raw_path
    )

    if not dry:
        all_votes_path = (
            preprocessed_path
            / f"compiled_votes_legislature_{legislature_id}.csv"
        )
        logger.debug(f"Writing to {all_votes_path}")

        df_all_votes.to_csv(all_votes_path, index=False)

        file = preprocessed_path / f"df_all_votes_{legislature_id}.parquet"
        logger.debug(f"Writing to {file}")
        df_all_votes.to_parquet(path=file)

    return df_all_votes
