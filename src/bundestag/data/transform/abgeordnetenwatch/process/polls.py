import json
import logging
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

import bundestag.schemas as schemas
from bundestag.data.utils import get_location, get_polls_filename

logger = logging.getLogger(__name__)


def load_polls_json(legislature_id: int, path: Path, dry: bool = False) -> dict:
    """Loads poll data from a JSON file for a given legislature.

    Args:
        legislature_id (int): The ID of the legislature for which to load poll data.
        path (Path): The path to the directory containing the poll data files.
        dry (bool, optional): If True, simulates file loading without reading the file. Defaults to False.

    Returns:
        dict: A dictionary containing the poll data from the JSON file.
    """
    polls_fname = get_polls_filename(legislature_id)
    file = get_location(polls_fname, path=path, dry=dry, mkdir=False)
    logger.debug(f"Reading poll info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_poll_data(poll: schemas.Poll) -> dict:
    """Parses a single poll object into a dictionary.

    This function extracts relevant information from a `schemas.Poll` object,
    including handling nested data like committees and cleaning HTML from descriptions.

    Args:
        poll (schemas.Poll): The poll object to parse.

    Returns:
        dict: A dictionary containing the parsed poll data.
    """
    handle_committee = (
        lambda x: None if x is None else None if len(x) == 0 else x[0].label
    )
    handle_description = (
        lambda x: BeautifulSoup(x, features="html.parser").get_text().strip()
    )

    d = {
        "poll_id": poll.id,
        "poll_title": poll.label,
        "poll_first_committee": handle_committee(poll.field_committees),
        "poll_description": handle_description(poll.field_intro),
        "legislature_id": poll.field_legislature.id,
        "legislature_period": poll.field_legislature.label,
        "poll_date": poll.field_poll_date,
    }
    return d


SCHEMA_GET_POLLS_DATA = pl.Schema(
    {
        "poll_id": pl.Int64(),
        "poll_title": pl.String(),
        "poll_first_committee": pl.String(),
        "poll_description": pl.String(),
        "legislature_id": pl.Int64(),
        "legislature_period": pl.String(),
        "poll_date": pl.String(),
    }
)


def get_polls_data(legislature_id: int, path: Path) -> pl.DataFrame:
    """Parses poll information from a JSON file and returns it as a Polars DataFrame.

    Args:
        legislature_id (int): The ID of the legislature for which to parse poll data.
        path (Path): The path to the directory containing the poll data files.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the parsed poll data.
    """

    info = load_polls_json(legislature_id, path=path)
    polls = schemas.PollResponse(**info)
    df = pl.DataFrame(
        [parse_poll_data(v) for v in polls.data], schema=SCHEMA_GET_POLLS_DATA
    )
    return df
