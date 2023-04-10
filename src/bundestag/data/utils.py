import json
import re
import typing as T
from pathlib import Path

import bundestag.logging as logging

logger = logging.logger

RE_HTM = re.compile("(\.html?)")
RE_FNAME = re.compile("(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")


def get_file_paths(
    path: T.Union[Path, str], suffix: str = None, pattern: re.Pattern = None
) -> T.List[Path]:
    """Collecting files with matching suffix or pattern

    Args:
        path (T.Union[Path, str]): Location to search for files
        suffix (str, optional): Suffix to search for. Defaults to None.
        pattern (re.Pattern, optional): Pattern to search for. Defaults to None.

    Raises:
        NotImplementedError: Function fails if neither suffix nor pattern are provided

    Returns:
        T.List[Path]: List of matched files
    """

    path = Path(path)
    if suffix is not None:
        logger.debug(f"Collecting using {suffix=}")
        files = list(path.rglob(suffix))
    elif pattern is not None:
        logger.debug(f"Collecting using {pattern=}")
        files = [f for f in path.glob("**/*") if pattern.search(f.name)]
    else:
        raise NotImplementedError(
            f"Either suffix or pattern need to be passed to this function."
        )
    files = list(set(files))
    return files


def get_sheet_fname(uri: str) -> str:
    return uri.split("/")[-1]


def polls_file(legislature_id: int):
    return f"polls_legislature_{legislature_id}.json"


def mandates_file(legislature_id: int):
    return f"mandates_legislature_{legislature_id}.json"


def votes_file(legislature_id: int, poll_id: int):
    return f"votes_legislature_{legislature_id}/poll_{poll_id}_votes.json"


def get_location(
    fname: str, path: Path, dry: bool = False, mkdir: bool = False
) -> Path:
    file = path / fname
    if (not dry) and mkdir:
        file.parent.mkdir(exist_ok=True)
    return file


def load_json(path: Path = None, dry: bool = False):
    logger.debug(f"Reading json info from {path=}")
    if dry:
        return {}
    with open(path, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def get_user_path_creation_decision(path: Path, max_tries: int = 3) -> bool:
    "Asks user if they want to create a path if it does not exist"

    msg = lambda x: f"Incorrect input {resp}, please enter y or n"
    for _ in range(max_tries):
        resp = input(f"Create {path.absolute()=}? ([y]/n) ")

        if not isinstance(resp, str) and resp is not None:
            logger.error(msg(resp))
            continue

        elif resp is None or len(resp) == 0:
            do_creation = True
            _msg = (
                "proceeding with download" if do_creation else "terminating."
            )
            logger.info(f"Received: {resp}, {_msg}")
            return do_creation

        elif resp.lower() in ["y", "n"]:
            do_creation = resp.lower() == "y"
            _msg = (
                "proceeding with download" if do_creation else "terminating."
            )
            logger.info(f"Received: {resp}, {_msg}")
            return do_creation

        else:
            logger.error(msg(resp))

    raise ValueError(f"Received {max_tries} incorrect inputs, terminating.")


def ensure_path_exists(path: Path):
    do_creation = get_user_path_creation_decision(path, max_tries=3)

    if do_creation:
        path.mkdir(exist_ok=True, parents=True)
