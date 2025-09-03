import json
import logging
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

import bundestag.schemas as schemas
from bundestag.data.utils import get_location, get_polls_filename

logger = logging.getLogger(__name__)


def load_polls_json(legislature_id: int, path: Path, dry: bool = False) -> dict:
    polls_fname = get_polls_filename(legislature_id)
    file = get_location(polls_fname, path=path, dry=dry, mkdir=False)
    logger.debug(f"Reading poll info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_poll_data(poll: schemas.Poll) -> dict:
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


def get_polls_data(legislature_id: int, path: Path) -> pl.DataFrame:
    "Parses info from poll json files for `legislature_id`"
    info = load_polls_json(legislature_id, path=path)
    polls = schemas.PollResponse(**info)
    df = pl.DataFrame([parse_poll_data(v) for v in polls.data])
    return df
