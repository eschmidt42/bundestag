from pathlib import Path

import pandas as pd

import bundestag.data.utils as data_utils
import bundestag.logging as logging
from bundestag.data.transform.abgeordnetenwatch.helper import (
    get_parties_from_col,
    get_politician_names,
)
from bundestag.data.transform.abgeordnetenwatch.process import (
    compile_votes_data,
    get_mandates_data,
    get_polls_data,
)

logger = logging.logger


def transform_mandates_data(df: pd.DataFrame) -> pd.DataFrame:
    df["all_parties"] = df.apply(get_parties_from_col, axis=1)
    df["party"] = df["all_parties"].apply(lambda x: x[-1])
    return df


def transform_votes_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{"politician name": get_politician_names})
    return df


def run(
    legislature_id: int,
    dry: bool = False,
    raw_path: Path = None,
    preprocessed_path: Path = None,
    validate: bool = False,
) -> pd.DataFrame:
    logger.info("Start transforming abgeordnetenwatch data")

    if not dry and (raw_path is None or preprocessed_path is None):
        raise ValueError(
            f"When {dry=} `raw_path` and or `preprocessed_path` cannot be None."
        )

    # ensure paths exist
    if not dry and not raw_path.exists():
        raise ValueError(f"{raw_path=} doesn't exist, terminating transformation.")
    if not dry and not preprocessed_path.exists():
        data_utils.ensure_path_exists(preprocessed_path)

    # polls
    df = get_polls_data(legislature_id, path=raw_path)
    if not dry:
        file = preprocessed_path / f"df_polls_{legislature_id}.parquet"
        logger.debug(f"writing to {file}")
        df.to_parquet(path=file)

    # mandates
    df = get_mandates_data(legislature_id, path=raw_path)
    df = transform_mandates_data(df)

    if not dry:
        file = preprocessed_path / f"df_mandates_{legislature_id}.parquet"
        logger.debug(f"Writing to {file}")
        df.to_parquet(path=file)

    # votes
    df_all_votes = compile_votes_data(legislature_id, path=raw_path, validate=validate)
    df_all_votes = transform_votes_data(df_all_votes)

    if not dry:
        all_votes_path = (
            preprocessed_path / f"compiled_votes_legislature_{legislature_id}.csv"
        )
        logger.debug(f"Writing to {all_votes_path}")

        df_all_votes.to_csv(all_votes_path, index=False)

        file = preprocessed_path / f"df_all_votes_{legislature_id}.parquet"
        logger.debug(f"Writing to {file}")
        df_all_votes.to_parquet(path=file)

    logger.info("Done transforming abgeordnetenwatch data")
