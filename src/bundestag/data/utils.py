import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

RE_HTM = re.compile(r"(\.html?)")
RE_FNAME = re.compile(r"(\.xlsx?)")
RE_SHEET = re.compile(r"(XLSX?)")


def get_file_paths(
    path: Path | str,
    pattern: re.Pattern | None = None,
    suffix: str | None = None,
) -> list[Path]:
    """Collects file paths from a directory based on a suffix or a regex pattern.

    This function recursively searches a given path for files. It can filter files
    either by a simple suffix (e.g., '*.txt') or by a regular expression pattern
    matched against the filename.

    Args:
        path (Path | str): The directory path to search for files.
        pattern (re.Pattern, optional): A regex pattern to match against file names. Defaults to None.
        suffix (str, optional): A suffix pattern (e.g., '*.json') to glob for. Defaults to None.

    Raises:
        NotImplementedError: If neither `suffix` nor `pattern` is provided.

    Returns:
        list[Path]: A list of unique `Path` objects for the matched files.
    """

    path = Path(path)
    if suffix is not None:
        logger.debug(f"Collecting using {suffix=}")
        files = list(path.rglob(suffix))
    elif pattern is not None:
        logger.debug(f"Collecting using {pattern=}")
        all_files = [f for f in path.glob("**/*") if f.is_file()]
        files = [f for f in all_files if pattern.search(f.name)]
    else:
        raise NotImplementedError(
            f"Either suffix or pattern need to be passed to this function."
        )
    files = list(set(files))
    return files


def get_sheet_filename(uri: str) -> str:
    """Extracts the filename from a URI.

    Args:
        uri (str): The URI to extract the filename from.

    Returns:
        str: The extracted filename.
    """
    return uri.split("/")[-1]


def get_polls_filename(legislature_id: int) -> str:
    """Generates the filename for a polls JSON file.

    Args:
        legislature_id (int): The ID of the legislature.

    Returns:
        str: The generated filename.
    """
    return f"polls_legislature_{legislature_id}.json"


def get_mandates_filename(legislature_id: int) -> str:
    """Generates the filename for a mandates JSON file.

    Args:
        legislature_id (int): The ID of the legislature.

    Returns:
        str: The generated filename.
    """
    return f"mandates_legislature_{legislature_id}.json"


def get_votes_filename(legislature_id: int, poll_id: int) -> str:
    """Generates the filename for a votes JSON file.

    Args:
        legislature_id (int): The ID of the legislature.
        poll_id (int): The ID of the poll.

    Returns:
        str: The generated filename, including the subdirectory for the legislature.
    """
    return f"votes_legislature_{legislature_id}/poll_{poll_id}_votes.json"


def get_location(
    fname: str, path: Path, dry: bool = False, mkdir: bool = False
) -> Path:
    """Constructs a full file path and optionally creates the parent directory.

    Args:
        fname (str): The name of the file.
        path (Path): The base directory path.
        dry (bool, optional): If True, directory creation is skipped. Defaults to False.
        mkdir (bool, optional): If True, the parent directory of the file is created if it doesn't exist. Defaults to False.

    Returns:
        Path: The full path to the file.
    """
    file = path / fname
    if (not dry) and mkdir:
        file.parent.mkdir(exist_ok=True)
    return file


def load_json(path: Path, dry: bool = False) -> dict:
    """Loads data from a JSON file.

    Args:
        path (Path): The path to the JSON file.
        dry (bool, optional): If True, returns an empty dictionary without reading the file. Defaults to False.

    Returns:
        dict: The data loaded from the JSON file, or an empty dictionary if in dry mode.
    """
    logger.debug(f"Reading json info from {path=}")
    if dry:
        return {}
    with open(path, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def get_user_path_creation_decision(path: Path, max_tries: int = 3) -> bool:
    """Asks the user for confirmation to create a directory path.

    This function prompts the user with a "y/n" question about whether a specified
    directory path should be created. It handles user input and retries on invalid input.

    Args:
        path (Path): The path to be created.
        max_tries (int, optional): The maximum number of times to prompt the user for input. Defaults to 3.

    Raises:
        ValueError: If the user provides incorrect input for `max_tries` consecutive times.

    Returns:
        bool: True if the user agrees to create the path, False otherwise.
    """

    msg = lambda x: f"Incorrect input {resp}, please enter y or n"
    for _ in range(max_tries):
        resp = input(f"Create {path.absolute()=}? ([y]/n) ")

        if not isinstance(resp, str) and resp is not None:
            logger.error(msg(resp))
            continue

        elif resp is None or len(resp) == 0:
            _msg = "proceeding with download"
            logger.info(f"Received: {resp}, {_msg}")
            return True

        elif resp.lower() in ["y", "n"]:
            do_creation = resp.lower() == "y"
            _msg = "proceeding with download" if do_creation else "terminating."
            logger.info(f"Received: {resp}, {_msg}")
            return do_creation

        else:
            logger.error(msg(resp))

    raise ValueError(f"Received {max_tries} incorrect inputs, terminating.")


def ensure_path_exists(path: Path, assume_yes: bool):
    """Ensures that a directory path exists, creating it if necessary.

    This function checks if a path exists. If not, it either prompts the user for
    confirmation to create it or creates it directly if `assume_yes` is True.

    Args:
        path (Path): The directory path to ensure exists.
        assume_yes (bool): If True, creates the path without prompting the user.
    """
    if assume_yes:
        do_creation = True
    else:
        do_creation = get_user_path_creation_decision(path, max_tries=3)

    if do_creation:
        path.mkdir(exist_ok=True, parents=True)


def file_size_is_zero(file: Path) -> bool:
    """Checks if a file's size is zero.

    Args:
        file (Path): The path to the file to check.

    Returns:
        bool: True if the file size is 0, False otherwise.
    """
    file_size = file.stat().st_size
    if file_size == 0:
        logger.warning(f"{file=} is of size 0, skipping ...")
        return True
    return False
