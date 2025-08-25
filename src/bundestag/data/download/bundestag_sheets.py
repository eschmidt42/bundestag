import json
import logging
import re
import time
from enum import StrEnum, auto
from pathlib import Path

import httpx
import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from bundestag.data.utils import ensure_path_exists, get_file_paths, get_sheet_filename

logger = logging.getLogger(__name__)

RE_HTM = re.compile(r"(\.html?)")
RE_FNAME = re.compile(r"(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")


def get_title_and_href(
    soup: BeautifulSoup, pattern: re.Pattern | None = None
) -> dict[str, str]:
    uris = {}
    elements = soup.find_all("td", attrs={"data-th": "Dokument"})
    for element in elements:
        title = element.div.p.strong.text.strip()  # type: ignore

        if pattern is not None:
            attrs = {"title": pattern}
        else:
            attrs = {}

        hrefs = element.find_all("a", attrs=attrs, href=True)  # type: ignore
        if hrefs is None:
            continue

        hrefs = [h for h in hrefs if h is not None]
        xlsx_hrefs = [
            h
            for h in hrefs
            if h["href"].endswith(".xlsx") or h["href"].endswith(".xls")  # type: ignore
        ]

        href = None
        if len(xlsx_hrefs) > 0:
            href = xlsx_hrefs[0]

        if href is None:
            continue

        uris[title] = href["href"]  # type:ignore
    return uris


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

    if pattern is None:
        pattern = RE_SHEET

    uris = {}
    for file_path in tqdm.tqdm(
        html_file_paths, total=len(html_file_paths), desc="HTM(L)"
    ):
        with open(file_path, "r") as f:
            soup = BeautifulSoup(f, features="html.parser")

        uris.update(get_title_and_href(soup, pattern))

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


def create_xlsx_uris_dict(max_pages: int = 3) -> dict[str, str]:
    logger.info("Collecting entries for xlsx_uris.json")

    # Initialize the WebDriver (e.g., Chrome)
    logger.info("Starting webdriver")
    driver = webdriver.Chrome()

    # Open the target page
    logger.info("Accessing page")
    driver.get("https://www.bundestag.de/parlament/plenum/abstimmung/liste")

    # List to store all xlsx URIs
    xlsx_uris = {}

    for i in range(max_pages):
        logger.info(f"page {i} of at mot {max_pages}")

        # Get the page source and parse it with BeautifulSoup
        logger.info(f"get soup")
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find all <a> tags with href ending in .xlsx
        logger.info(f"get links")

        new_xlsx_uris = get_title_and_href(soup)

        logger.info(f"found {len(new_xlsx_uris):_} new uris")
        xlsx_uris.update(new_xlsx_uris)
        logger.info(f"total uris now {len(xlsx_uris):_}")

        try:
            # Find and click the "next" button
            logger.info(f"finding the button")
            next_button = driver.find_element(By.CSS_SELECTOR, "button.slick-next")
            if next_button.get_attribute("aria-disabled") == "true":
                logger.info(f"button disabled, exiting")
                break  # Exit the loop if the button is disabled

            # Scroll to the button
            logger.info(f"scrolling to the button")
            driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center'});", next_button
            )
            time.sleep(1)  # Allow time for scrolling

            logger.info(f"clicking the button")
            next_button.click()
            time.sleep(2)  # Wait for the next page to load

        except Exception as e:
            logger.error(f"Error: {e}")
            break

    # Close the browser
    logger.info(f"closing browser")
    driver.quit()

    logger.info(f"Found {len(xlsx_uris)} .xlsx URIs.")

    return xlsx_uris


def store_xlsx_uris(json_path: Path, xlsx_uris: dict[str, str]):
    logger.info(f"Writing to {json_path}")
    with json_path.open("w") as f:
        json.dump(xlsx_uris, f, indent=4)


class Source(StrEnum):
    html_file = auto()
    json_file = auto()


def run(
    html_dir: Path,
    sheet_dir: Path,
    t_sleep: float = 0.01,
    nmax: int | None = None,
    dry: bool = False,
    pattern: re.Pattern = RE_HTM,
    assume_yes: bool = False,
    source: Source = Source.html_file,
    json_filename: str = "xlsx_uris.json",
    do_create_xlsx_uris_json: bool = False,
    max_pages: int = 5,
):
    logger.info("Start downloading bundestag sheets")

    # ensure paths exist
    if not html_dir.exists():
        ensure_path_exists(html_dir, assume_yes=assume_yes)
    if not sheet_dir.exists():
        ensure_path_exists(sheet_dir, assume_yes=assume_yes)

    match source:
        case Source.html_file:
            html_file_paths = get_file_paths(html_dir, pattern=pattern)
            sheet_uris = collect_sheet_uris(html_file_paths)
        case Source.json_file:
            json_path = html_dir.parent / json_filename

            if do_create_xlsx_uris_json:
                xlsx_uris = create_xlsx_uris_dict(max_pages=max_pages)
                store_xlsx_uris(json_path, xlsx_uris)

            if not json_path.exists():
                raise ValueError(
                    f"Running this function with {source=} requires that {json_path=} exists."
                )
            with json_path.open("r") as f:
                sheet_uris = json.load(f)

    download_multiple_sheets(
        sheet_uris, sheet_dir=sheet_dir, t_sleep=t_sleep, nmax=nmax, dry=dry
    )

    logger.info("Done downloading bundestag sheets")
