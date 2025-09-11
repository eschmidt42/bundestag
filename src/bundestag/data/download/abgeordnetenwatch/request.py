import logging

import httpx

logger = logging.getLogger(__name__)


def request_poll_data(
    legislature_id: int, dry: bool = False, num_polls: int = 999, timeout: float = 42
) -> dict | None:
    """Request poll data from abgeordnetenwatch.de.

    Args:
        legislature_id (int): The ID of the legislature to request poll data for.
        dry (bool, optional): If True, simulates the request without making an actual HTTP call. Defaults to False.
        num_polls (int, optional): The maximum number of polls to retrieve. Defaults to 999.
        timeout (float, optional): The timeout for the HTTP request in seconds. Defaults to 42.

    Returns:
        dict | None: A dictionary containing the poll data, or None if in dry mode.
    """

    url = "https://www.abgeordnetenwatch.de/api/v2/polls"
    params = {
        "field_legislature": legislature_id,  # Bundestag period 2017-2021 = 111
        "range_end": num_polls,  # setting a high limit to include all polls in one go
    }

    if dry:
        logger.info(f"Dry mode - request setup: url = {url}, params = {params}")
        return

    r = httpx.get(url, params=params, timeout=timeout)

    logger.info(f"Requested {r.url} ({r.status_code=})")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()


def request_mandates_data(
    legislature_id: int, dry=False, num_mandates: int = 999, timeout: float = 42
) -> dict | None:
    """Request mandates data from abgeordnetenwatch.de.

    Args:
        legislature_id (int): The ID of the legislature to request mandates data for.
        dry (bool, optional): If True, simulates the request without making an actual HTTP call. Defaults to False.
        num_mandates (int, optional): The maximum number of mandates to retrieve. Defaults to 999.
        timeout (float, optional): The timeout for the HTTP request in seconds. Defaults to 42.

    Returns:
        dict | None: A dictionary containing the mandates data, or None if in dry mode.
    """

    url = f"https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates"
    params = {
        "parliament_period": legislature_id,  # collecting parlamentarians' votes
        "range_end": num_mandates,  # setting a high limit to include all mandates in one go
    }
    if dry:
        logger.info(f"Dry mode - request setup: url = {url}, params = {params}")
        return

    r = httpx.get(url, params=params, timeout=timeout)
    logger.info(f"Requested {r.url} ({r.status_code=})")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()


def request_vote_data(
    poll_id: int, dry=False, timeout: float = 42, num_votes: int = 999
) -> dict | None:
    """Request votes data from abgeordnetenwatch.de

    Args:
        poll_id (int): The ID of the poll to request vote data for.
        dry (bool, optional): If True, simulates the request without making an actual HTTP call. Defaults to False.
        timeout (float, optional): The timeout for the HTTP request in seconds. Defaults to 42.
        num_votes (int, optional): The maximum number of votes to retrieve. Defaults to 999.

    Returns:
        dict | None: A dictionary containing the vote data, or None if in dry mode.
    """

    url = f"https://www.abgeordnetenwatch.de/api/v2/polls/{poll_id}"
    params = {
        "related_data": "votes",
        "range_end": num_votes,
    }  # collecting parlamentarians' votes
    if dry:
        logger.debug(f"Dry mode - request setup: url = {url}, params = {params}")
        return

    r = httpx.get(url, params=params, timeout=timeout)

    logger.debug(f"Requested {r.url}")
    assert r.status_code == 200, f"Unexpected GET status: {r.status_code}"

    return r.json()
