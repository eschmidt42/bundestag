import logging
from pathlib import Path

import pandas as pd

from bundestag.data.transform.abgeordnetenwatch.helper import (
    get_parties_from_col,
    get_politician_names,
)
from bundestag.data.transform.abgeordnetenwatch.process import (
    compile_votes_data,
    get_mandates_data,
    get_polls_data,
)
from bundestag.data.utils import ensure_path_exists

logger = logging.getLogger(__name__)


def transform_mandates_data(df: pd.DataFrame) -> pd.DataFrame:
    df["all_parties"] = df.apply(get_parties_from_col, axis=1)
    df["party"] = df["all_parties"].apply(lambda x: x[-1])
    return df


def transform_votes_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{"politician name": get_politician_names})
    return df


def get_votes_parquet_path(legislature_id: int, preprocessed_path: Path):
    return preprocessed_path / f"votes_{legislature_id}.parquet"


def get_votes_csv_path(legislature_id: int, preprocessed_path: Path):
    return preprocessed_path / f"votes_{legislature_id}.csv"


def get_mandates_parquet_path(legislature_id: int, preprocessed_path: Path):
    return preprocessed_path / f"mandates_{legislature_id}.parquet"


def get_polls_parquet_path(legislature_id: int, preprocessed_path: Path):
    return preprocessed_path / f"polls_{legislature_id}.parquet"


def run(
    legislature_id: int,
    raw_path: Path,
    preprocessed_path: Path,
    dry: bool,
    validate: bool = False,
    assume_yes: bool = False,
):
    logger.info("Start transforming abgeordnetenwatch data")

    if not dry and (raw_path is None or preprocessed_path is None):
        raise ValueError(
            f"When {dry=} `raw_path` and or `preprocessed_path` cannot be None."
        )

    # ensure paths exist
    if not dry and not raw_path.exists():
        raise ValueError(f"{raw_path=} doesn't exist, terminating transformation.")
    if not dry and not preprocessed_path.exists():
        ensure_path_exists(preprocessed_path, assume_yes=assume_yes)

    # polls
    df = get_polls_data(legislature_id, path=raw_path)
    if not dry:
        file = get_polls_parquet_path(legislature_id, preprocessed_path)
        logger.info(f"writing to {file}")
        df.to_parquet(path=file)

    # mandates
    df = get_mandates_data(legislature_id, path=raw_path)
    df = transform_mandates_data(df)

    if not dry:
        file = get_mandates_parquet_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {file}")
        df.to_parquet(path=file)

    # votes
    df_all_votes = compile_votes_data(legislature_id, raw_path, validate=validate)
    df_all_votes = transform_votes_data(df_all_votes)

    if not dry:
        all_votes_path = get_votes_csv_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {all_votes_path}")

        df_all_votes.to_csv(all_votes_path, index=False)

        file = get_votes_parquet_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {file}")
        df_all_votes.to_parquet(path=file)

    logger.info("Done transforming abgeordnetenwatch data")
