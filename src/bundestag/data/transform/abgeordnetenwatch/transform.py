import logging
from pathlib import Path
from time import perf_counter

import polars as pl

from bundestag.data.transform.abgeordnetenwatch.helper import (
    get_parties_from_col,
)
from bundestag.data.transform.abgeordnetenwatch.process import (
    compile_votes_data,
    get_mandates_data,
    get_polls_data,
)
from bundestag.data.utils import ensure_path_exists

logger = logging.getLogger(__name__)


def transform_mandates_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transforms mandates data by extracting party information.

    This function adds two new columns to the DataFrame:
    - 'all_parties': A list of all parties a politician has been a member of, extracted from the 'fraction_names' column.
    - 'party': The most recent party of the politician, taken as the last element from the 'all_parties' list.

    Args:
        df (pl.DataFrame): The input DataFrame containing mandates data.

    Returns:
        pl.DataFrame: The transformed DataFrame with added party information.
    """
    df = df.with_columns(
        **{"all_parties": pl.col("fraction_names").map_elements(get_parties_from_col)}
    ).with_columns(**{"party": pl.col("all_parties").list.last()})

    return df


def transform_votes_data(df: pl.DataFrame) -> pl.DataFrame:
    """Transforms votes data by extracting the politician's name.

    This function adds a 'politician name' column to the DataFrame by extracting the name
    from the 'mandate' column, which typically has the format "Politician Name (Party)".

    Args:
        df (pl.DataFrame): The input DataFrame containing votes data.

    Returns:
        pl.DataFrame: The transformed DataFrame with the added 'politician name' column.
    """
    df = df.with_columns(
        **{"politician name": pl.col("mandate").str.extract(r"^(.*?) \s*\(", 1)}
    )
    return df


def get_votes_parquet_path(legislature_id: int, preprocessed_path: Path) -> Path:
    """Constructs the file path for the votes Parquet file.

    Args:
        legislature_id (int): The ID of the legislature.
        preprocessed_path (Path): The path to the directory for preprocessed data.

    Returns:
        Path: The full path to the votes Parquet file.
    """
    return preprocessed_path / f"votes_{legislature_id}.parquet"


def get_votes_csv_path(legislature_id: int, preprocessed_path: Path) -> Path:
    """Constructs the file path for the votes CSV file.

    Args:
        legislature_id (int): The ID of the legislature.
        preprocessed_path (Path): The path to the directory for preprocessed data.

    Returns:
        Path: The full path to the votes CSV file.
    """
    return preprocessed_path / f"votes_{legislature_id}.csv"


def get_mandates_parquet_path(legislature_id: int, preprocessed_path: Path) -> Path:
    """Constructs the file path for the mandates Parquet file.

    Args:
        legislature_id (int): The ID of the legislature.
        preprocessed_path (Path): The path to the directory for preprocessed data.

    Returns:
        Path: The full path to the mandates Parquet file.
    """
    return preprocessed_path / f"mandates_{legislature_id}.parquet"


def get_polls_parquet_path(legislature_id: int, preprocessed_path: Path) -> Path:
    """Constructs the file path for the polls Parquet file.

    Args:
        legislature_id (int): The ID of the legislature.
        preprocessed_path (Path): The path to the directory for preprocessed data.

    Returns:
        Path: The full path to the polls Parquet file.
    """
    return preprocessed_path / f"polls_{legislature_id}.parquet"


def run(
    legislature_id: int,
    raw_path: Path,
    preprocessed_path: Path,
    dry: bool,
    validate: bool = False,
    assume_yes: bool = False,
):
    """Runs the full data transformation pipeline for abgeordnetenwatch data for a given legislature.

    This function performs the following steps:
    1. Loads and processes polls data, then saves it as a Parquet file.
    2. Loads, transforms, and processes mandates data, then saves it as a Parquet file.
    3. Compiles and transforms votes data, then saves it as both CSV and Parquet files.

    Args:
        legislature_id (int): The ID of the legislature to process.
        raw_path (Path): The path to the directory containing the raw data.
        preprocessed_path (Path): The path to the directory where the preprocessed data will be saved.
        dry (bool): If True, the function will only log the actions it would take without writing any files.
        validate (bool, optional): A flag for validation during vote compilation. Defaults to False.
        assume_yes (bool, optional): If True, it will automatically create the preprocessed path if it doesn't exist. Defaults to False.

    Raises:
        ValueError: If `dry` is False and either `raw_path` or `preprocessed_path` is not provided.
        ValueError: If `raw_path` does not exist.
    """
    logger.info(f"Start transforming abgeordnetenwatch data for {legislature_id=}")
    start_time = perf_counter()

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
        df.write_parquet(file)

    # mandates
    df = get_mandates_data(legislature_id, path=raw_path)
    df = transform_mandates_data(df)

    if not dry:
        file = get_mandates_parquet_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {file}")
        df.write_parquet(file)

    # votes
    df_all_votes = compile_votes_data(legislature_id, raw_path, validate=validate)
    df_all_votes = transform_votes_data(df_all_votes)

    if not dry:
        all_votes_path = get_votes_csv_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {all_votes_path}")

        df_all_votes.write_csv(all_votes_path)

        file = get_votes_parquet_path(legislature_id, preprocessed_path)
        logger.info(f"Writing to {file}")
        df_all_votes.write_parquet(file)

    dt = perf_counter() - start_time
    logger.info(
        f"Done transforming abgeordnetenwatch data for {legislature_id=} after {dt}"
    )
