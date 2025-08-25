import logging

logger = logging.getLogger(__name__)


def get_user_download_decision(n: int, max_tries: int = 3) -> bool:
    "Ask user if they want to download n polls"

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
