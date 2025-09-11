import logging
import time
from enum import StrEnum
from pathlib import Path
from time import perf_counter

from scipy import stats
from tqdm import tqdm

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
from bundestag.data.utils import ensure_path_exists

logger = logging.getLogger(__name__)


def identify_remaining_poll_ids(
    possible_ids: list[int],
    known_ids: dict[int, Path],
) -> list[int]:
    """Identifies poll IDs that have not yet been downloaded.

    Args:
        possible_ids (list[int]): A list of all possible poll IDs for a given legislature.
        known_ids (dict[int, Path]): A dictionary of poll IDs that have already been downloaded,
                                     mapping poll ID to the path of the stored data.

    Returns:
        list[int]: A list of poll IDs that still need to be downloaded.
    """
    return [v for v in possible_ids if v not in known_ids]


def request_and_store_poll_ids(
    dt_rv_scale: float,
    remaining_poll_ids: list[int],
    dry: bool,
    t_sleep: float,
    path: Path,
    random_state: int = 42,
    timeout: float = 42.0,
):
    """Loops over remaining poll ids and requests them individually with random sleep times.

    Args:
        dt_rv_scale (float): The scale parameter for the normal distribution used to generate random sleep times.
        remaining_poll_ids (list[int]): A list of poll IDs to be downloaded.
        dry (bool): If True, the function will not actually download any data, but will only log the actions it would have taken.
        t_sleep (float): The base sleep time between requests.
        path (Path): The path to the directory where the downloaded data should be stored.
        random_state (int, optional): The random seed for the random number generator. Defaults to 42.
        timeout (float, optional): The timeout for the HTTP requests. Defaults to 42.0.
    """

    dt_rv = stats.norm(scale=dt_rv_scale)

    logger.info(
        f"Starting requests for {len(remaining_poll_ids)} remaining polls ({dry=})"
    )

    for poll_id in tqdm(
        remaining_poll_ids, total=len(remaining_poll_ids), desc="poll_id"
    ):
        # random sleep time
        _t = t_sleep + abs(dt_rv.rvs(random_state=random_state))
        if not dry:
            time.sleep(_t)

        # collect vote data
        data = request_vote_data(poll_id, dry=dry, timeout=timeout)

        # store vote data
        store_vote_json(path, data, poll_id, dry=dry)

    logger.info("Done with requests for remaining polls")


def get_all_remaining_vote_data(
    legislature_id: int,
    path: Path,
    dry: bool = False,
    t_sleep: float = 1,
    dt_rv_scale: float = 0.1,
    ask_user: bool = True,
    timeout: float = 42,
):
    """Loop through the remaining polls for `legislature_id` to collect all votes and write them to disk.

    Args:
        legislature_id (int): The ID of the legislature to download data for.
        path (Path): The path to the directory where the downloaded data should be stored.
        dry (bool, optional): If True, the function will not actually download any data, but will only log the actions it would have taken. Defaults to False.
        t_sleep (float, optional): The base sleep time between requests. Defaults to 1.
        dt_rv_scale (float, optional): The scale parameter for the normal distribution used to generate random sleep times. Defaults to 0.1.
        ask_user (bool, optional): If True, the user will be prompted for confirmation before downloading the data. Defaults to True.
        timeout (float, optional): The timeout for the HTTP requests. Defaults to 42.
    """
    logger.info("Collecting remaining vote data")

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
    logger.info(
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

    request_and_store_poll_ids(
        dt_rv_scale, remaining_poll_ids, dry, t_sleep, path, timeout=timeout
    )


class EntityEnum(StrEnum):
    mandate = "candidacies-mandates"
    poll = "polls"
    vote = "vote"
    all = "all"


def run(
    legislature_id: int,
    dry: bool = False,
    raw_path: Path = Path("data/abgeordnetenwatch"),
    max_polls: int = 999,
    max_mandates: int = 999,
    t_sleep: float = 1,
    dt_rv_scale: float = 0.1,
    ask_user: bool = True,
    assume_yes: bool = False,
    entity: EntityEnum = EntityEnum.all,
    timeout: float = 42.0,
):
    """Run the abgeordnetenwatch data collection pipeline for the given legislature id.

    Args:
        legislature_id (int): The ID of the legislature to download data for.
        dry (bool, optional): If True, the function will not actually download any data, but will only log the actions it would have taken. Defaults to False.
        raw_path (Path, optional): The path to the directory where the downloaded data should be stored. Defaults to Path("data/abgeordnetenwatch").
        max_polls (int, optional): The maximum number of polls to download. Defaults to 999.
        max_mandates (int, optional): The maximum number of mandates to download. Defaults to 999.
        t_sleep (float, optional): The base sleep time between requests. Defaults to 1.
        dt_rv_scale (float, optional): The scale parameter for the normal distribution used to generate random sleep times. Defaults to 0.1.
        ask_user (bool, optional): If True, the user will be prompted for confirmation before downloading the data. Defaults to True.
        assume_yes (bool, optional): If True, the function will assume the user has answered "yes" to any prompts. Defaults to False.
        entity (EntityEnum, optional): The type of data to download. Defaults to EntityEnum.all.
        timeout (float, optional): The timeout for the HTTP requests. Defaults to 42.0.

    Raises:
        ValueError: If `dry` is False and `raw_path` is not provided.
    """
    start_time = perf_counter()
    logger.info(
        f"Start downloading abgeordnetenwatch {entity=} data for {legislature_id=}"
    )

    if not dry and (raw_path is None):
        raise ValueError(f"When {dry=} `raw_path` cannot be None.")

    # ensure paths exist
    if not dry and not raw_path.exists():
        ensure_path_exists(raw_path, assume_yes=assume_yes)

    # polls
    if entity in [EntityEnum.all, EntityEnum.poll]:
        data = request_poll_data(
            legislature_id, dry=dry, num_polls=max_polls, timeout=timeout
        )
        store_polls_json(raw_path, data, legislature_id, dry=dry)

    # mandates
    if entity in [EntityEnum.all, EntityEnum.mandate]:
        data = request_mandates_data(
            legislature_id, dry=dry, num_mandates=max_mandates, timeout=timeout
        )
        store_mandates_json(raw_path, data, legislature_id, dry=dry)

    # votes
    if entity in [EntityEnum.all, EntityEnum.vote]:
        get_all_remaining_vote_data(
            legislature_id,
            raw_path,
            dry=dry,
            t_sleep=t_sleep,
            dt_rv_scale=dt_rv_scale,
            ask_user=ask_user,
            timeout=timeout,
        )
    dt = str(perf_counter() - start_time)
    logger.info(
        f"Done downloading abgeordnetenwatch data for {legislature_id=} after {dt}."
    )
