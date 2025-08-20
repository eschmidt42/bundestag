import json
import typing as T
from pathlib import Path

import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger


def store_polls_json(
    path: Path, polls: dict | None, legislature_id: int, dry: bool = False
):
    "Write poll data to file"

    file = data_utils.get_location(
        data_utils.polls_file(legislature_id), path=path, dry=dry, mkdir=False
    )

    if dry:
        logger.debug(f"Dry mode - Writing poll info to {file}")
        return

    logger.debug(f"Writing poll info to {file}")
    with open(file, "w", encoding="utf8") as f:
        json.dump(polls, f)


def store_mandates_json(
    path: Path, mandates: dict | None, legislature_id: int, dry: bool = False
):
    "Write mandates data to file"

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


def store_vote_json(path: Path, votes: dict | None, poll_id: int, dry=False):
    "Write votes data to file"

    if dry:
        _votes_file = data_utils.votes_file(42, poll_id)
        _location = data_utils.get_location(
            _votes_file, path=path, dry=dry, mkdir=False
        )
        logger.debug(f"Dry mode - Writing votes info to {_location}")
        return
    if votes is None:
        raise ValueError(f"votes cannot be None for {dry=}")

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


def list_votes_dirs(path: Path) -> dict[int, Path]:
    "List all votes_legislature_* directories"

    dir2int = lambda x: int(str(x).split("_")[-1])

    # get all legislature ids for which there are directories present
    vote_dirs = list(path.glob("votes_legislature_*"))
    if len(vote_dirs) == 0:
        logger.error(f"No vote directories found in {path}")
        return {}

    # create a dict with legislature ids as keys and the corresponding file paths as values
    legislature_ids = {dir2int(v): v for v in vote_dirs}

    return legislature_ids


def list_polls_files(legislature_id: int, path: Path) -> dict[int, Path]:
    "List all polls_legislature_* files"

    file2int = lambda x: int(str(x).split("_")[-2])

    leg_path = path / f"votes_legislature_{legislature_id}"

    # check if the path actually exists
    if not leg_path.exists():
        logger.error(
            f"No vote directory found for legislature {legislature_id} in {path}"
        )
        return {}

    # get all poll ids for which there are files present
    poll_ids = {file2int(v): v for v in leg_path.glob("poll_*_votes.json")}
    return poll_ids


def check_stored_vote_ids(
    legislature_id: int | None, path: Path
) -> T.Dict[int, T.Dict[int, Path]]:
    "Check which vote ids are already stored"

    legislature_ids = list_votes_dirs(path=path)

    # determine if the legislature id is known
    leg_id_unknown = (
        legislature_id is not None and legislature_id not in legislature_ids
    )

    if leg_id_unknown:
        # if the legislature id is unknown, there are no associated files, hence return an empty dict
        logger.error(
            f"Given legislature_id {legislature_id} is unknown. Known ids: {sorted(list(legislature_ids.keys()))}"
        )
        if legislature_id is not None:
            return {legislature_id: {}}
        else:
            return {}

    elif legislature_id is not None:
        # if the legislature id is known, return the associated files
        # this is the common case

        vote_ids = list_polls_files(legislature_id, path=path)

        return {legislature_id: vote_ids}

    else:
        # if the legislature id is None, return all files
        all_ids = {}
        for leg_id in legislature_ids:
            all_ids[leg_id] = list_polls_files(leg_id, path=path)

        return all_ids


def check_possible_poll_ids(
    legislature_id: int, path: Path, dry: bool = False
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
    data = data_utils.load_json(polls_file, dry=dry)

    if dry:
        return []

    polls = schemas.PollResponse(**data)

    poll_ids = list(set([v.id for v in polls.data]))

    logger.debug(f"Identified {len(poll_ids)} unique poll ids")

    return poll_ids
