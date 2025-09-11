import json
import logging
from pathlib import Path

import polars as pl

import bundestag.schemas as schemas
from bundestag.data.utils import get_location, get_mandates_filename

logger = logging.getLogger(__name__)


def load_mandate_json(legislature_id: int, path: Path, dry: bool = False) -> dict:
    """Loads mandate data from a JSON file for a given legislature.

    Args:
        legislature_id (int): The ID of the legislature for which to load mandate data.
        path (Path): The path to the directory containing the mandate data files.
        dry (bool, optional): If True, simulates file loading without reading the file. Defaults to False.

    Returns:
        dict: A dictionary containing the mandate data from the JSON file.
    """
    mandates_fname = get_mandates_filename(legislature_id)
    file = get_location(
        mandates_fname,
        path=path,
        dry=dry,
        mkdir=False,
    )

    logger.debug(f"Reading mandates info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_mandate_data(mandate: schemas.Mandate, missing: str = "unknown") -> dict:
    """Parses a single mandate object into a dictionary.

    Args:
        mandate (schemas.Mandate): The mandate object to parse.
        missing (str, optional): The value to use for missing fraction data. Defaults to "unknown".

    Returns:
        dict: A dictionary containing the parsed mandate data.
    """
    d = {
        "legislature_id": mandate.parliament_period.id,
        "legislature_period": mandate.parliament_period.label,
        "mandate_id": mandate.id,
        "mandate": mandate.label,
        "politician_id": mandate.politician.id,
        "politician": mandate.politician.label,
        "politician_url": mandate.politician.abgeordnetenwatch_url,
        "start_date": mandate.start_date,
        "end_date": "" if mandate.end_date is None else mandate.end_date,
        "constituency_id": mandate.electoral_data.constituency.id
        if mandate.electoral_data.constituency is not None
        else None,
        "constituency_name": mandate.electoral_data.constituency.label
        if mandate.electoral_data.constituency is not None
        else None,
    }

    if len(mandate.fraction_membership) > 0:
        d_fraction = {
            "fraction_names": [_m.label for _m in mandate.fraction_membership],
            "fraction_ids": [_m.id for _m in mandate.fraction_membership],
            "fraction_starts": [_m.valid_from for _m in mandate.fraction_membership],
            "fraction_ends": [
                "" if _m.valid_until is None else _m.valid_until
                for _m in mandate.fraction_membership
            ],
        }
    else:
        d_fraction = {
            "fraction_names": [missing],
            "fraction_ids": [missing],
            "fraction_starts": [missing],
            "fraction_ends": [missing],
        }
    d.update(d_fraction)
    return d


SCHEMA_GET_MANDATES_DATA = pl.Schema(
    {
        "legislature_id": pl.Int64(),
        "legislature_period": pl.String(),
        "mandate_id": pl.Int64(),
        "mandate": pl.String(),
        "politician_id": pl.Int64(),
        "politician": pl.String(),
        "politician_url": pl.String(),
        "start_date": pl.String(),
        "end_date": pl.String(),
        "constituency_id": pl.Int64(),
        "constituency_name": pl.String(),
        "fraction_names": pl.List(pl.String()),
        "fraction_ids": pl.List(pl.Int64()),
        "fraction_starts": pl.List(pl.String()),
        "fraction_ends": pl.List(pl.String()),
    }
)


def get_mandates_data(legislature_id: int, path: Path) -> pl.DataFrame:
    """Parses mandate information from a JSON file and returns it as a Polars DataFrame.

    Args:
        legislature_id (int): The ID of the legislature for which to parse mandate data.
        path (Path): The path to the directory containing the mandate data files.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the parsed mandate data.
    """

    info = load_mandate_json(legislature_id, path=path)
    mandates = schemas.MandatesResponse(**info)
    df = pl.DataFrame(
        [parse_mandate_data(m) for m in mandates.data], schema=SCHEMA_GET_MANDATES_DATA
    )
    return df
