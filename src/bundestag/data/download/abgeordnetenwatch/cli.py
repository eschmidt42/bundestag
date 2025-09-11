import logging

logger = logging.getLogger(__name__)


def get_user_download_decision(n: int, max_tries: int = 3) -> bool:
    """Ask user if they want to download n polls.

    Args:
        n (int): The number of polls to be downloaded.
        max_tries (int, optional): The maximum number of times the user can be prompted for input. Defaults to 3.

    Raises:
        ValueError: If the user provides incorrect input for `max_tries` consecutive times.

    Returns:
        bool: True if the user agrees to download, False otherwise.
    """

    msg = lambda x: f"Incorrect input {resp}, please enter y or n"

    for _ in range(max_tries):
        resp = input(f"Download {n} polls? ([y]/n) ")

        if resp is None or (isinstance(resp, str) and len(resp) == 0):
            do_download = True
            _msg = "proceeding with download" if do_download else "terminating."
            logger.info(f"Received: {resp}, {_msg}")
            return do_download

        elif not isinstance(resp, str):
            logger.error(msg(resp))
            continue

        elif resp.lower() in ["y", "n"]:
            do_download = resp.lower() == "y"
            _msg = "proceeding with download" if do_download else "terminating."
            logger.info(f"Received: {resp}, {_msg}")
            return do_download

        else:
            logger.error(msg(resp))

    raise ValueError(f"Received {max_tries} incorrect inputs, terminating.")
