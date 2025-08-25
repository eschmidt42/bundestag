import logging
import re
import time
from pathlib import Path

import httpx
import tqdm
from bs4 import BeautifulSoup

from bundestag.data.utils import ensure_path_exists, get_file_paths, get_sheet_filename

logger = logging.getLogger(__name__)

RE_HTM = re.compile(r"(\.html?)")
RE_FNAME = re.compile(r"(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")


def collect_sheet_uris(
    html_file_paths: list[Path], pattern: re.Pattern | None = None
) -> dict[str, str]:
    """Extracting URIs to roll call votes stored in excel sheets

    Args:
        html_file_paths (list[Path]): List of files to parse
        pattern (re.Pattern, optional): Regular expression pattern to use to identify URIs. Defaults to None.

    Returns:
        list[str]: List of identified URIs
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
            title = element.div.p.strong.text.strip()  # type: ignore
            href = element.find("a", attrs={"title": pattern})  # type: ignore
            if href is None:
                continue
            uris[title] = href["href"]  # type:ignore
    return uris


def get_sheet_path(uri: str, sheet_dir: str | Path) -> Path:
    return Path(sheet_dir) / get_sheet_filename(uri)


def download_sheet(uri: str, sheet_dir: Path, dry: bool = False):
    """Downloads a single excel sheet given `uri` and writes to `sheet_path`

    Args:
        uri (str): URI to download
        sheet_path (Path | str): Directory to write downloaded sheet to
        dry (bool, optional): Switch to deactivate downloading. Defaults to False.
    """

    "Downloads a single excel sheet given `uri` and writes to `sheet_path`"

    if dry:
        return

    r = httpx.get(uri)

    sheet_path = get_sheet_path(uri, sheet_dir)
    logger.debug(f"Writing requesting excel sheet: {uri} and writing to {sheet_path}")

    sheet_dir.mkdir(exist_ok=True, parents=True)
    with sheet_path.open("wb") as f:
        f.write(r.content)


def download_multiple_sheets(
    uris: dict[str, str],
    sheet_dir: Path,
    t_sleep: float = 0.01,
    nmax: int | None = None,
    dry: bool = False,
):
    """Downloads multiple excel sheets containing roll call votes using `uris`, writing to `sheet_path`

    Args:
        uris (dict[str, str]): Dict of sheets to download
        sheet_path (Path | str): Path to write the sheets to
        t_sleep (float, optional): Wait time in seconds between downloaded files. Defaults to 0.01.
        nmax (int, optional): Maximum number of sheets to download. Defaults to None.
        dry (bool, optional): Switch for dry run. Defaults to False.
    """

    n = min(nmax, len(uris)) if nmax else len(uris)
    logger.info(
        f"Downloading {n} excel sheets and storing under {sheet_dir} (dry = {dry})"
    )
    known_sheets = get_file_paths(sheet_dir, pattern=RE_FNAME)
    t0 = time.perf_counter()

    for i, (_, uri) in tqdm.tqdm(enumerate(uris.items()), desc="File", total=n):
        if nmax is not None and i > nmax - 1:
            break

        sheet_path = get_sheet_path(uri, sheet_dir)

        sheet_is_known = sheet_path in known_sheets
        if sheet_is_known:
            continue

        download_sheet(uri, sheet_dir=sheet_dir, dry=dry)

        time.sleep(t_sleep)

    t1 = time.perf_counter()
    logger.info(
        f"Done downloading {n} excel sheets and storing under {sheet_dir} (dry = {dry}). Took {t1 - t0}."
    )


def run(
    html_dir: Path,
    sheet_dir: Path,
    t_sleep: float = 0.01,
    nmax: int | None = None,
    dry: bool = False,
    pattern: re.Pattern = RE_HTM,
    assume_yes: bool = False,
):
    logger.info("Start downloading bundestag sheets")

    # ensure paths exist
    if not html_dir.exists():
        ensure_path_exists(html_dir, assume_yes=assume_yes)
    if not sheet_dir.exists():
        ensure_path_exists(sheet_dir, assume_yes=assume_yes)

    html_file_paths = get_file_paths(html_dir, pattern=pattern)
    sheet_uris = collect_sheet_uris(html_file_paths)
    download_multiple_sheets(
        sheet_uris, sheet_dir=sheet_dir, t_sleep=t_sleep, nmax=nmax, dry=dry
    )

    logger.info("Done downloading bundestag sheets")
