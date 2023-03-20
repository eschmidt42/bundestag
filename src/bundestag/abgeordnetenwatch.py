import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from scipy import stats
from tqdm import tqdm

import bundestag.logging as logging

logger = logging.logger

API_ENCODING = "ISO-8859-1"


def get_location(
    fname: str, path: Path, dry: bool = False, mkdir: bool = False
) -> Path:
    file = path / fname
    if (not dry) and mkdir:
        file.parent.mkdir(exist_ok=True)
    return file


def get_poll_info(legislature_id: int, dry=False, num_polls: int = 999):
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


def polls_file(legislature_id: int):
    return f"polls_legislature_{legislature_id}.json"


def store_polls_json(
    polls: dict, legislature_id: int, dry: bool = False, path: Path = None
):
    file = get_location(
        polls_file(legislature_id), path=path, dry=dry, mkdir=False
    )

    if dry:
        logger.debug(f"Dry mode - Writing poll info to {file}")
        return
    logger.debug(f"Writing poll info to {file}")
    with open(file, "w", encoding="utf8") as f:
        json.dump(polls, f)


def load_polls_json(legislature_id: int, path: Path = None, dry: bool = False):
    file = get_location(
        polls_file(legislature_id), path=path, dry=dry, mkdir=False
    )
    logger.debug(f"Reading poll info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_poll_data(poll):
    handle_committee = (
        lambda x: None if x is None else None if len(x) == 0 else x[0]["label"]
    )
    handle_description = (
        lambda x: BeautifulSoup(x, features="html.parser").get_text().strip()
    )

    d = {
        "poll_id": poll["id"],
        "poll_title": poll["label"],
        "poll_first_committee": handle_committee(poll["field_committees"]),
        "poll_description": handle_description(poll["field_intro"]),
        "legislature_id": poll["field_legislature"]["id"],
        "legislature_period": poll["field_legislature"]["label"],
        "poll_date": poll["field_poll_date"],
    }
    return d


# TODO: add optional pandera validation
def get_polls_df(legislature_id: int, path: Path = None):
    "Parses info from poll json files for `legislature_id`"
    info = load_polls_json(legislature_id, path=path)
    return pd.DataFrame([parse_poll_data(v) for v in info["data"]])


def get_mandates_info(legislature_id: int, dry=False, num_mandates: int = 999):
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


def mandates_file(legislature_id: int):
    return f"mandates_legislature_{legislature_id}.json"


def store_mandates_info(
    mandates: dict, legislature_id: int, dry: bool = False, path: Path = None
):
    file = get_location(
        mandates_file(legislature_id), path=path, dry=dry, mkdir=False
    )
    # if path is None:
    #     path = ABGEORDNETENWATCH_PATH
    # file = (
    #     path / f"mandates_legislature_{legislature_id}.json"
    # )

    if dry:
        logger.debug(f"Dry mode - Writing mandates info to {file}")
        return
    logger.debug(f"Writing mandates info to {file}")
    with open(file, "w", encoding="utf8") as f:
        json.dump(mandates, f)


def load_mandate_json(
    legislature_id: int, path: Path = None, dry: bool = False
):
    file = get_location(
        mandates_file(legislature_id), path=path, dry=dry, mkdir=False
    )
    # if mandates_path is None:
    #     mandates_path = (
    #         ABGEORDNETENWATCH_PATH / f"mandates_legislature_{legislature_id}.json"
    #     )
    logger.debug(f"Reading mandates info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_mandate_data(m):
    handle_constituency = (
        lambda x, k: x["electoral_data"]["constituency"][k]
        if x["electoral_data"].get("constituency", None)
        else None
    )
    d = {
        "legislature_id": m["parliament_period"]["id"],
        "legislature_period": m["parliament_period"]["label"],
        "mandate_id": m["id"],
        "mandate": m["label"],
        "politician_id": m["politician"]["id"],
        "politician": m["politician"]["label"],
        "politician_url": m["politician"]["abgeordnetenwatch_url"],
        "start_date": m["start_date"],
        "end_date": m["end_date"],
        "constituency_id": handle_constituency(
            m, "id"
        ),  #  m['electoral_data']['constituency']['id']
        "constituency_name": handle_constituency(
            m, "label"
        ),  # m['electoral_data']['constituency']['label'],
    }
    if "fraction_membership" in m:
        d.update(
            {
                "fraction_names": [
                    _m["label"] for _m in m["fraction_membership"]
                ],
                "fraction_ids": [_m["id"] for _m in m["fraction_membership"]],
                "fraction_starts": [
                    _m["valid_from"] for _m in m["fraction_membership"]
                ],
                "fraction_ends": [
                    _m["valid_until"] for _m in m["fraction_membership"]
                ],
            }
        )
    return d


def get_mandates_df(legislature_id: int, test: bool = True, path: Path = None):
    "Parses info from mandate json file(s) for `legislature_id`"
    info = load_mandate_json(legislature_id, path=path)
    df = pd.DataFrame([parse_mandate_data(v) for v in info["data"]])
    # if test:
    #     test_mandate_data(df)
    return df


def get_vote_info(poll_id: int, dry=False):
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


def votes_file(legislature_id: int, poll_id: int):
    return f"votes_legislature_{legislature_id}/poll_{poll_id}_votes.json"


def store_vote_info(votes: dict, poll_id: int, dry=False, path: Path = None):
    if dry:
        logger.debug(
            f"Dry mode - Writing votes info to {get_location(votes_file(None, poll_id), path=path, dry=dry, mkdir=False)}"
        )
        return

    legislature_id = votes["data"]["field_legislature"]["id"]
    file = get_location(
        votes_file(legislature_id, poll_id), path=path, dry=dry, mkdir=True
    )

    # if votes_path is None:
    #     votes_path = ABGEORDNETENWATCH_PATH / f"votes_legislature_{legislature_id}"
    # votes_path.mkdir(exist_ok=True)
    # votes_path = votes_path / f"poll_{poll_id}_votes.json"

    logger.debug(f"Writing votes info to {file}")

    with open(file, "w", encoding="utf8") as f:
        json.dump(votes, f)


def load_vote_json(legislature_id: int, poll_id: int, path: Path = None):
    # legislature_id = votes["data"]["field_legislature"]["id"]
    file = get_location(
        votes_file(legislature_id, poll_id), path=path, dry=False, mkdir=False
    )
    # votes_path = (
    #     ABGEORDNETENWATCH_PATH
    #     / f"votes_legislature_{legislature_id}/poll_{poll_id}_votes.json"
    # )
    logger.debug(f"Reading vote info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_vote_data(vote):
    d = {
        "mandate_id": vote["mandate"]["id"],
        "mandate": vote["mandate"]["label"],
        "poll_id": vote["poll"]["id"],
        "vote": vote["vote"],
        "reason_no_show": vote["reason_no_show"],
        "reason_no_show_other": vote["reason_no_show_other"],
    }
    return d


# TODO: add optional pandera schema validation
def get_votes_df(legislature_id: int, poll_id: int, path: Path = None):
    "Parses info from vote json files for `legislature_id` and `poll_id`"
    info = load_vote_json(legislature_id, poll_id, path=path)
    df = pd.DataFrame(
        [parse_vote_data(v) for v in info["data"]["related_data"]["votes"]]
    )
    # if test:
    #     test_vote_data(df)
    return df


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


def get_all_remaining_vote_info(
    legislature_id: int,
    dry: bool = False,
    t_sleep: float = 1,
    dt_rv_scale: float = 0.1,
    test: bool = True,
    path: Path = None,
):
    "Loop through the remaining polls for `legislature_id` to collect all votes and write them to disk."

    # Get known legislature_id / poll_id combinations
    known_id_combos = check_stored_vote_ids(
        legislature_id=legislature_id, path=path
    )

    # Get polls info for legislative period
    df_period = get_polls_df(legislature_id, path=path)

    # remaining poll ids to collect
    remaining_poll_ids = [
        v
        for v in df_period["poll_id"].unique()
        if v not in known_id_combos[legislature_id]
    ]
    logger.debug(
        f"remaining poll_ids (legislature_id = {legislature_id}) = {len(remaining_poll_ids)}:\n{remaining_poll_ids}"
    )

    dt_rv = stats.norm(scale=dt_rv_scale)

    for i, poll_id in enumerate(
        tqdm(remaining_poll_ids, total=len(remaining_poll_ids), desc="poll_id")
    ):
        _t = t_sleep + abs(dt_rv.rvs())
        if not dry:
            time.sleep(_t)
        info = get_vote_info(poll_id, dry=dry)
        store_vote_info(info, poll_id, dry=dry, path=path)
    logger.debug(
        f"vote collection for legislature_id {legislature_id} complete (dry = {dry})"
    )


def compile_votes_data(legislature_id: int, path: Path = None):
    "Compiles the individual politicians' votes for a specific legislature period"

    known_id_combos = check_stored_vote_ids(
        legislature_id=legislature_id, path=path
    )

    # TODO: figure out why some mandate_id entries are duplicate in vote_json files

    df_all_votes = []
    for poll_id in tqdm(
        known_id_combos[legislature_id],
        total=len(known_id_combos[legislature_id]),
        desc="poll_id",
    ):
        df = get_votes_df(legislature_id, poll_id, test=False, path=path)

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

    return pd.concat(df_all_votes, ignore_index=True)


PARTY_PATTERN = re.compile("(.+)\sseit")


def get_party_from_fraction_string(row, col="fraction_names"):
    x = row[col]
    if not isinstance(x, list):
        return "unknown"
    elif len(x) > 0 and "seit" not in x[0]:
        return x[0]
    else:
        return PARTY_PATTERN.search(x[0]).groups()[0]


def get_politician_names(df: pd.DataFrame, col="mandate"):
    names = df[col].str.split(" ").str[:-4].str.join(" ")
    logger.debug(f"Parsing `{col}` to names. Found {names.nunique()} names")
    return names
