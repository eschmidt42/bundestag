import json
import logging
from pathlib import Path

import pandas as pd
from tqdm import tqdm

import bundestag.data.utils as data_utils
import bundestag.schemas as schemas
from bundestag.data.download.abgeordnetenwatch.store import check_stored_vote_ids

logger = logging.getLogger(__name__)


def load_vote_json(legislature_id: int, poll_id: int, path: Path) -> dict:
    votes_fname = data_utils.votes_file(legislature_id, poll_id)
    file = data_utils.get_location(
        votes_fname,
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


def get_votes_data(
    legislature_id: int,
    poll_id: int,
    path: Path,
    validate: bool = False,
) -> pd.DataFrame:
    "Parses info from vote json files for `legislature_id` and `poll_id`"
    data = load_vote_json(legislature_id, poll_id, path=path)
    votes = schemas.VoteResponse(**data)
    n_none = 0
    df = []
    for vote in votes.data.related_data.votes:
        if vote.id is None:
            n_none += 1
            continue
        vote = parse_vote_data(vote)
        df.append(vote)

    if n_none > 0:
        logger.warning(f"Removed {n_none} votes because of their id being None")
    df = pd.DataFrame(df)  # [parse_vote_data(v) for v in votes.data.related_data.votes]

    if validate:
        schemas.VOTES.validate(df)

    return df


def compile_votes_data(legislature_id: int, path: Path, validate: bool = False):
    "Compiles the individual politicians' votes for a specific legislature period"

    known_id_combos = check_stored_vote_ids(legislature_id=legislature_id, path=path)

    # TODO: figure out why some mandate_id entries are duplicate in vote_json files

    df_all_votes = []
    for poll_id in tqdm(
        known_id_combos[legislature_id],
        total=len(known_id_combos[legislature_id]),
        desc="poll_id",
    ):
        df = get_votes_data(legislature_id, poll_id, path=path, validate=False)

        ids = df.loc[df.duplicated(subset=["mandate_id"]), "mandate_id"].unique()
        if len(ids) > 0:
            logger.warning(
                f"Dropping duplicates for mandate_ids ({ids}):\n{df.loc[df['mandate_id'].isin(ids)]}"
            )
            df = df.drop_duplicates(subset=["mandate_id"])

        df_all_votes.append(df)

    df_all_votes = pd.concat(df_all_votes, ignore_index=True)

    if validate:
        schemas.VOTES.validate(df_all_votes)
    return df_all_votes
