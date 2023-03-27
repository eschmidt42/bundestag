import json
import re
import sys
import time
import typing as T
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field
from scipy import stats
from tqdm import tqdm

import bundestag.data.download.abgeordnetenwatch as download_aw
import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger


def load_polls_json(legislature_id: int, path: Path = None, dry: bool = False):
    file = data_utils.get_location(
        data_utils.polls_file(legislature_id), path=path, dry=dry, mkdir=False
    )
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


def get_polls_df(legislature_id: int, path: Path = None) -> pd.DataFrame:
    "Parses info from poll json files for `legislature_id`"
    info = load_polls_json(legislature_id, path=path)
    polls = schemas.PollResponse(**info)
    return pd.DataFrame([parse_poll_data(v) for v in polls.data])


def load_mandate_json(
    legislature_id: int, path: Path = None, dry: bool = False
) -> dict:
    file = data_utils.get_location(
        data_utils.mandates_file(legislature_id),
        path=path,
        dry=dry,
        mkdir=False,
    )

    logger.debug(f"Reading mandates info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_mandate_data(mandate: schemas.Mandate) -> dict:
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

    if mandate.fraction_membership is not None:
        d.update(
            {
                "fraction_names": [
                    _m.label for _m in mandate.fraction_membership
                ],
                "fraction_ids": [_m.id for _m in mandate.fraction_membership],
                "fraction_starts": [
                    _m.valid_from for _m in mandate.fraction_membership
                ],
                "fraction_ends": [
                    "" if _m.valid_until is None else _m.valid_until
                    for _m in mandate.fraction_membership
                ],
            }
        )
    return d


def get_mandates_df(legislature_id: int, path: Path = None) -> pd.DataFrame:
    "Parses info from mandate json file(s) for `legislature_id`"
    info = load_mandate_json(legislature_id, path=path)
    mandates = schemas.MandatesResponse(**info)
    df = pd.DataFrame([parse_mandate_data(m) for m in mandates.data])
    return df


PARTY_PATTERN = re.compile("(.+)\sseit")


def extract_party_from_string(s: str) -> str:
    if not isinstance(s, str):
        raise ValueError(f"Expected {s=} to be of type string.")
    elif "seit" in s:
        return PARTY_PATTERN.search(s).groups()[0]
    else:
        return s


def get_parties_from_col(
    row: pd.Series, col="fraction_names", missing: str = "unknown"
):
    strings = row[col]
    if strings is None or not isinstance(strings, list):
        return [missing]
    else:
        return [extract_party_from_string(s) for s in strings]


def transform_mandates_df(df: pd.DataFrame) -> pd.DataFrame:
    df["all_parties"] = df.apply(get_parties_from_col, axis=1)
    df["party"] = df["party"].apply(lambda x: x[-1])
    return df


def load_vote_json(
    legislature_id: int, poll_id: int, path: Path = None
) -> dict:
    file = data_utils.get_location(
        data_utils.votes_file(legislature_id, poll_id),
        path=path,
        dry=False,
        mkdir=False,
    )

    logger.debug(f"Reading vote info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_vote_data(vote: schemas.Vote) -> dict:
    d = {
        "mandate_id": vote.mandate.id,
        "mandate": vote.mandate.label,
        "poll_id": vote.poll.id,
        "vote": vote.vote,
        "reason_no_show": vote.reason_no_show,
        "reason_no_show_other": vote.reason_no_show_other,
    }

    return d


def get_votes_df(
    legislature_id: int,
    poll_id: int,
    path: Path = None,
    validate: bool = False,
) -> pd.DataFrame:
    "Parses info from vote json files for `legislature_id` and `poll_id`"
    info = load_vote_json(legislature_id, poll_id, path=path)
    votes = schemas.VoteResponse(**info)
    df = pd.DataFrame(
        [parse_vote_data(v) for v in votes.data.related_data.votes]
    )

    if validate:
        schemas.VOTES.validate(df)

    return df


def compile_votes_data(
    legislature_id: int, path: Path = None, validate: bool = False
):
    "Compiles the individual politicians' votes for a specific legislature period"

    known_id_combos = download_aw.check_stored_vote_ids(
        legislature_id=legislature_id, path=path
    )

    # TODO: figure out why some mandate_id entries are duplicate in vote_json files

    df_all_votes = []
    for poll_id in tqdm(
        known_id_combos[legislature_id],
        total=len(known_id_combos[legislature_id]),
        desc="poll_id",
    ):
        df = get_votes_df(legislature_id, poll_id, path=path, validate=False)

        ids = df.loc[
            df.duplicated(subset=["mandate_id"]), "mandate_id"
        ].unique()
        if len(ids) > 0:
            logger.warning(
                f'Dropping duplicates for mandate_ids ({ids}):\n{df.loc[df["mandate_id"].isin(ids)]}'
            )
            df = df.drop_duplicates(subset=["mandate_id"])
        # test_vote_data(df)

        df_all_votes.append(df)

    df_all_votes = pd.concat(df_all_votes, ignore_index=True)

    if validate:
        schemas.VOTES.validate(df_all_votes)
    return df_all_votes


def get_politician_names(df: pd.DataFrame, col="mandate"):
    names = df[col].str.split(" ").str[:-4].str.join(" ")
    logger.debug(f"Parsing `{col}` to names. Found {names.nunique()} names")
    return names


def transform_votes_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(**{"politician name": get_politician_names})
    return df
