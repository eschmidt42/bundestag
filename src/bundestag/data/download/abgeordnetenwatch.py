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

import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger

API_ENCODING = "ISO-8859-1"


def request_poll_data(
    legislature_id: int, dry: bool = False, num_polls: int = 999
) -> dict:
    url = "https://www.abgeordnetenwatch.de/api/v2/polls"
    params = {
        "field_legislature": legislature_id,  # Bundestag period 2017-2021 = 111
        "range_end": num_polls,  # setting a high limit to include all polls in one go
    }

    if dry:
        logger.debug(
            f"Dry mode - request setup: url = {url}, params = {params}"
        )
        return

    r = requests.get(url, params=params)

    logger.debug(f"Requested {r.url}")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()  # encoding=API_ENCODING


def store_polls_json(
    polls: dict, legislature_id: int, dry: bool = False, path: Path = None
):
    file = data_utils.get_location(
        data_utils.polls_file(legislature_id), path=path, dry=dry, mkdir=False
    )

    if dry:
        logger.debug(f"Dry mode - Writing poll info to {file}")
        return
    logger.debug(f"Writing poll info to {file}")
    with open(file, "w", encoding="utf8") as f:
        json.dump(polls, f)


def request_mandates_data(
    legislature_id: int, dry=False, num_mandates: int = 999
) -> dict:
    url = f"https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates"
    params = {
        "parliament_period": legislature_id,  # collecting parlamentarians' votes
        "range_end": num_mandates,  # setting a high limit to include all mandates in one go
    }
    if dry:
        logger.debug(
            f"Dry mode - request setup: url = {url}, params = {params}"
        )
        return

    r = requests.get(url, params=params)
    logger.debug(f"Requested {r.url}")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()  # encoding=API_ENCODING


def store_mandates_json(
    mandates: dict, legislature_id: int, dry: bool = False, path: Path = None
):
    file = data_utils.get_location(
        data_utils.mandates_file(legislature_id),
        path=path,
        dry=dry,
        mkdir=False,
    )

    if dry:
        logger.debug(f"Dry mode - Writing mandates info to {file}")
        return
    logger.debug(f"Writing mandates info to {file}")
    with open(file, "w", encoding="utf8") as f:
        json.dump(mandates, f)


def request_vote_data(poll_id: int, dry=False) -> dict:
    url = f"https://www.abgeordnetenwatch.de/api/v2/polls/{poll_id}"
    params = {"related_data": "votes"}  # collecting parlamentarians' votes
    if dry:
        logger.debug(
            f"Dry mode - request setup: url = {url}, params = {params}"
        )
        return

    r = requests.get(url, params=params)

    logger.debug(f"Requested {r.url}")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()  # encoding=API_ENCODING


def store_vote_json(votes: dict, poll_id: int, dry=False, path: Path = None):
    if dry:
        logger.debug(
            f"Dry mode - Writing votes info to {data_utils.get_location(data_utils.votes_file(None, poll_id), path=path, dry=dry, mkdir=False)}"
        )
        return

    legislature_id = votes["data"]["field_legislature"]["id"]
    file = data_utils.get_location(
        data_utils.votes_file(legislature_id, poll_id),
        path=path,
        dry=dry,
        mkdir=True,
    )

    logger.debug(f"Writing votes info to {file}")

    with open(file, "w", encoding="utf8") as f:
        json.dump(votes, f)


def check_stored_vote_ids(legislature_id: int, path: Path):
    dir2int = lambda x: int(str(x).split("_")[-1])
    legislature_ids = {dir2int(v): v for v in path.glob("votes_legislature_*")}

    file2int = lambda x: int(str(x).split("_")[-2])
    id_unknown = (
        legislature_id is not None and legislature_id not in legislature_ids
    )

    if id_unknown:
        logger.error(
            f"Given legislature_id {legislature_id} is unknown. Known ids: {sorted(list(legislature_ids.keys()))}"
        )
        return {legislature_id: {}}

    elif legislature_id is not None:
        vote_ids = {
            file2int(v): v
            for v in (path / f"votes_legislature_{legislature_id}").glob(
                "poll_*_votes.json"
            )
        }
        return {legislature_id: vote_ids}

    else:
        all_ids = {}
        for leg_id, leg_path in legislature_ids.items():
            all_ids[leg_id] = {
                file2int(v): v for v in leg_path.glob("poll_*_votes.json")
            }
        return all_ids


def get_user_download_decision(n: int, max_tries: int = 3) -> bool:
    msg = lambda x: f"Incorrect input {resp}, please enter y or n"
    for _ in range(max_tries):
        resp = input(f"Download {n} polls? ([y]/n) ")
        if resp is None or len(resp) == 0:
            do_dowload = True
            _msg = "proceeding with download" if do_dowload else "terminating."
            logger.info(f"Received: {resp}, {_msg}")
            return do_dowload
        elif resp.lower() in ["y", "n"]:
            do_dowload = resp.lower() == "y"
            _msg = "proceeding with download" if do_dowload else "terminating."
            logger.info(f"Received: {resp}, {_msg}")
            return do_dowload
        elif not isinstance(resp, str):
            logger.error(msg(resp))
            continue
        else:
            logger.error(msg(resp))

    raise ValueError(f"Received {max_tries} incorrect inputs, terminating.")


def check_possible_poll_ids(
    legislature_id: int, path: Path = None, dry: bool = False
) -> T.List[int]:
    """Collect available poll ids for given legislature id

    Args:
        legislature_id (int): Legislature identifier
        path (Path, optional): Path to poll files. Defaults to None.
        dry (bool, optional): Dry or not. Defaults to False.

    Returns:
        T.List[int]: List of poll identifiers
    """
    polls_file = data_utils.polls_file(legislature_id)
    polls_file = path / polls_file
    logger.debug(f"Reading {polls_file=}")
    info = data_utils.load_json(polls_file, dry=dry)
    if dry:
        return []
    polls = schemas.PollResponse(**info)
    poll_ids = list(set([v.id for v in polls.data]))
    logger.debug(f"Identified {len(poll_ids)} unique poll ids")
    return poll_ids


def get_all_remaining_vote_data(
    legislature_id: int,
    dry: bool = False,
    t_sleep: float = 1,
    dt_rv_scale: float = 0.1,
    path: Path = None,
    ask_user: bool = True,
):
    "Loop through the remaining polls for `legislature_id` to collect all votes and write them to disk."

    # Get known legislature_id / poll_id combinations
    known_id_combos = check_stored_vote_ids(
        legislature_id=legislature_id, path=path
    )

    # Get polls info for legislative period
    possible_poll_ids = check_possible_poll_ids(
        legislature_id=legislature_id, path=path, dry=dry
    )

    # remaining poll ids to collect
    remaining_poll_ids = [
        v
        for v in possible_poll_ids
        if v not in known_id_combos[legislature_id]
    ]
    n = len(remaining_poll_ids)
    logger.debug(
        f"remaining poll_ids (legislature_id = {legislature_id}) = {n}:\n{remaining_poll_ids}"
    )
    if n == 0:
        logger.info("Nothing to download, returning.")
        return

    # getting input from the user
    if ask_user:
        do_download = get_user_download_decision(n)
    else:
        do_download = True

    if not do_download:
        return

    dt_rv = stats.norm(scale=dt_rv_scale)

    for i, poll_id in enumerate(
        tqdm(remaining_poll_ids, total=len(remaining_poll_ids), desc="poll_id")
    ):
        _t = t_sleep + abs(dt_rv.rvs())
        if not dry:
            time.sleep(_t)
        info = request_vote_data(poll_id, dry=dry)
        store_vote_json(info, poll_id, dry=dry, path=path)
    logger.debug(
        f"vote collection for legislature_id {legislature_id} complete (dry = {dry})"
    )


def run(
    legislature_id: int,
    dry: bool = False,
    raw_path: Path = None,
    preprocessed_path: Path = None,
    max_polls: int = 999,
    max_mandates: int = 999,
) -> pd.DataFrame:
    logger.info("Start downloading abgeordnetenwatch data")

    if not dry and (raw_path is None or preprocessed_path is None):
        raise ValueError(
            f"When {dry=} `raw_path` and or `preprocessed_path` cannot be None."
        )

    # ensure paths exist
    if not raw_path.exists():
        data_utils.ensure_path_exists(raw_path)

    # polls
    data = request_poll_data(legislature_id, dry=dry, num_polls=max_polls)
    store_polls_json(data, legislature_id, dry=dry, path=raw_path)

    # mandates
    data = request_mandates_data(
        legislature_id, dry=dry, num_mandates=max_mandates
    )
    store_mandates_json(data, legislature_id, dry=dry, path=raw_path)

    # votes
    get_all_remaining_vote_data(legislature_id, dry=dry, path=raw_path)

    logger.info("Done downloading abgeordnetenwatch data!")
