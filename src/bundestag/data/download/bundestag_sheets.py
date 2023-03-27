import re
import time
import typing as T
from pathlib import Path

import requests
import tqdm
from bs4 import BeautifulSoup

import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger

RE_HTM = re.compile("(\.html?)")
RE_FNAME = re.compile("(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")


def collect_sheet_uris(
    html_file_paths: T.List[Path], pattern: re.Pattern = None
) -> T.List[str]:
    """Extracting URIs to roll call votes stored in excel sheets

    Args:
        html_file_paths (T.List[Path]): List of files to parse
        pattern (re.Pattern, optional): Regular expression pattern to use to identify URIs. Defaults to None.

    Returns:
        T.List[str]: List of identified URIs
    """

    logger.info("Extracting URIs to excel sheets from htm files")
    uris = {}
    if pattern is None:
        pattern = RE_SHEET

    for file_path in tqdm.tqdm(
        html_file_paths, total=len(html_file_paths), desc="HTM(L)"
    ):
        with open(file_path, "r") as f:
            soup = BeautifulSoup(f, features="html.parser")

        elements = soup.find_all("td", attrs={"data-th": "Dokument"})
        for element in elements:
            title = element.div.p.strong.text.strip()
            href = element.find("a", attrs={"title": pattern})
            if href is None:
                continue
            uris[title] = href["href"]
    return uris


def download_sheet(
    uri: str, sheet_path: T.Union[Path, str], dry: bool = False
):
    """Downloads a single excel sheet given `uri` and writes to `sheet_path`

    Args:
        uri (str): URI to download
        sheet_path (T.Union[Path, str]): Directory to write downloaded sheet to
        dry (bool, optional): Switch to deactivate downloading. Defaults to False.
    """

    "Downloads a single excel sheet given `uri` and writes to `sheet_path`"
    sheet_path.mkdir(exist_ok=True)
    file = Path(sheet_path) / uri.split("/")[-1]
    logger.debug(
        f"Writing requesting excel sheet: {uri} and writing to {file}"
    )
    if dry:
        return
    with open(file, "wb") as f:
        r = requests.get(uri)
        f.write(r.content)


def download_multiple_sheets(
    uris: T.Dict[str, str],
    sheet_path: T.Union[Path, str],
    t_sleep: float = 0.01,
    nmax: int = None,
    dry: bool = False,
):
    """Downloads multiple excel sheets containing roll call votes using `uris`, writing to `sheet_path`

    Args:
        uris (T.Dict[str, str]): Dict of sheets to download
        sheet_path (T.Union[Path, str]): Path to write the sheets to
        t_sleep (float, optional): Wait time in seconds between downloaded files. Defaults to 0.01.
        nmax (int, optional): Maximum number of sheets to download. Defaults to None.
        dry (bool, optional): Switch for dry run. Defaults to False.
    """

    n = min(nmax, len(uris)) if nmax else len(uris)
    logger.info(
        f"Downloading {n} excel sheets and storing under {sheet_path} (dry = {dry})"
    )
    known_sheets = data_utils.get_file_paths(sheet_path, pattern=RE_FNAME)

    for i, (_, uri) in tqdm.tqdm(
        enumerate(uris.items()), desc="File", total=n
    ):
        if nmax is not None and i > nmax:
            break
        fname = data_utils.get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            continue

        download_sheet(uri, sheet_path=sheet_path, dry=dry)

        time.sleep(t_sleep)
