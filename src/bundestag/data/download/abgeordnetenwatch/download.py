import time
import typing as T
from pathlib import Path

from scipy import stats
from tqdm import tqdm

import bundestag.data.utils as data_utils
import bundestag.logging as logging
from bundestag.data.download.abgeordnetenwatch.cli import get_user_download_decision
from bundestag.data.download.abgeordnetenwatch.request import (
    request_mandates_data,
    request_poll_data,
    request_vote_data,
)
from bundestag.data.download.abgeordnetenwatch.store import (
    check_possible_poll_ids,
    check_stored_vote_ids,
    store_mandates_json,
    store_polls_json,
    store_vote_json,
)

logger = logging.logger


def identify_remaining_poll_ids(
    possible_ids: T.List[int], known_ids: T.List[int]
) -> T.List[int]:
    return [v for v in possible_ids if v not in known_ids]


def request_and_store_poll_ids(
    dt_rv_scale: float,
    remaining_poll_ids: T.List[int],
    dry: bool,
    t_sleep: float,
    path: Path,
):
    "Loops over remaining poll ids and request them individually with random sleep times"

    dt_rv = stats.norm(scale=dt_rv_scale)

    logger.debug(
        f"Starting requests for {len(remaining_poll_ids)} remaining polls ({dry=})"
    )

    for poll_id in tqdm(
        remaining_poll_ids, total=len(remaining_poll_ids), desc="poll_id"
    ):
        # random sleep time
        _t = t_sleep + abs(dt_rv.rvs())
        if not dry:
            time.sleep(_t)

        # collect vote data
        data = request_vote_data(poll_id, dry=dry)

        # store vote data
        store_vote_json(data, poll_id, dry=dry, path=path)

    logger.debug("Done with requests forr remaining polls")


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
    known_id_combos = check_stored_vote_ids(legislature_id=legislature_id, path=path)

    # Get polls info for legislative period
    possible_poll_ids = check_possible_poll_ids(
        legislature_id=legislature_id, path=path, dry=dry
    )

    # remaining poll ids to collect
    known_poll_ids = known_id_combos[legislature_id]
    remaining_poll_ids = identify_remaining_poll_ids(possible_poll_ids, known_poll_ids)

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

    request_and_store_poll_ids(dt_rv_scale, remaining_poll_ids, dry, t_sleep, path)


def run(
    legislature_id: int,
    dry: bool = False,
    raw_path: Path | None = None,
    max_polls: int = 999,
    max_mandates: int = 999,
    t_sleep: float = 1,
    dt_rv_scale: float = 0.1,
    ask_user: bool = True,
    assume_yes: bool = False,
):
    "Run the abgeordnetenwatch data collection pipeline for the given legislature id."

    logger.info("Start downloading abgeordnetenwatch data")

    if not dry and (raw_path is None):
        raise ValueError(f"When {dry=} `raw_path` cannot be None.")

    # ensure paths exist
    if not dry and not raw_path.exists():
        data_utils.ensure_path_exists(raw_path, assume_yes=assume_yes)

    # polls
    data = request_poll_data(legislature_id, dry=dry, num_polls=max_polls)
    store_polls_json(data, legislature_id, dry=dry, path=raw_path)

    # mandates
    data = request_mandates_data(legislature_id, dry=dry, num_mandates=max_mandates)
    store_mandates_json(data, legislature_id, dry=dry, path=raw_path)

    # votes
    get_all_remaining_vote_data(
        legislature_id,
        dry=dry,
        t_sleep=t_sleep,
        dt_rv_scale=dt_rv_scale,
        path=raw_path,
        ask_user=ask_user,
    )

    logger.info("Done downloading abgeordnetenwatch data!")
