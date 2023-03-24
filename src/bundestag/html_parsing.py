import re
import time
import typing as T
from pathlib import Path

import pandas as pd
import requests
import tqdm
from bs4 import BeautifulSoup

import bundestag.logging as logging
import bundestag.schemas as schemas

# from fastcore.all import *


logger = logging.logger

# default level for this module should be INFO

RE_HTM = re.compile("(\.html?)")
RE_FNAME = re.compile("(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")
VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]

# TODO: add dry mode for http requests
# TODO: add logger debug statements


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


def get_sheet_fname(uri: str) -> str:
    return uri.split("/")[-1]


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
    known_sheets = get_file_paths(sheet_path, pattern=RE_FNAME)

    for i, (_, uri) in tqdm.tqdm(
        enumerate(uris.items()), desc="File", total=n
    ):
        if nmax is not None and i > nmax:
            break
        fname = get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            continue

        download_sheet(uri, sheet_path=sheet_path, dry=dry)

        time.sleep(t_sleep)


def get_file2poll_maps(
    uris: T.Dict[str, str], sheet_path: T.Union[str, Path]
) -> T.Dict[str, str]:
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = get_file_paths(sheet_path, pattern=RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            file2poll[fname] = poll_title
    return file2poll


def is_date(s: str, fun: T.Callable):
    try:
        _ = fun(s)
        return True
    except:
        return False


# TODO: implement optional pandera schema validation
def get_sheet_df(
    sheet_file: T.Union[str, Path],
    file_title_maps: T.Dict[str, str] = None,
    validate: bool = False,
):
    "Parsing xlsx and xls files into dataframes"

    if Path(sheet_file).stat().st_size == 0:
        logger.warning(f"{sheet_file} is of size 0, skipping ...")
        return

    dfs = pd.read_excel(sheet_file, sheet_name=None)

    assert (
        len(dfs) == 1
    ), "The sheet file has more than one page, that's unexpected."

    for name, df in dfs.items():
        df["sheet_name"] = name

    assert not (df[VOTE_COLS].sum(axis=1) == 0).any()
    assert not (df[VOTE_COLS].sum(axis=1) > 1).any()

    date, title = None, None
    if file_title_maps is not None:
        title, date = handle_title_and_date(
            file_title_maps[sheet_file.name], sheet_file
        )

    df["date"] = date
    df["title"] = title

    df = df.pipe(disambiguate_party)

    if validate:
        schemas.SHEET.validate(df)

    return df


def handle_title_and_date(
    full_title: str, sheet_file: Path
) -> T.Tuple[str, pd.DatetimeTZDtype]:
    "Extracting the title of the roll call vote and the date"
    title = full_title.split(":")
    date = title[0]
    if is_date(date, lambda x: pd.to_datetime(x, dayfirst=True)):
        date = pd.to_datetime(date, dayfirst=True)
        title = ":".join(title[1:])
    elif is_date(sheet_file.name.split("_")[0], pd.to_datetime):
        date = pd.to_datetime(sheet_file.name.split("_")[0])
        title = full_title
    else:
        date = None
        title = full_title

    title = title.strip()
    return title, date


PARTY_MAP = {
    "BÜNDNIS`90/DIE GRÜNEN": "BÜ90/GR",
    "DIE LINKE": "DIE LINKE.",
    "fraktionslos": "Fraktionslos",
    "fraktionslose": "Fraktionslos",
}


def disambiguate_party(
    df: pd.DataFrame, col: str = "Fraktion/Gruppe", party_map: dict = None
) -> pd.DataFrame:
    if party_map is None:
        party_map = PARTY_MAP
    df[col] = df[col].apply(
        lambda x: x if x not in party_map else party_map[x]
    )
    return df


# TODO: add pandera schema validation
def get_squished_dataframe(
    df: pd.DataFrame,
    id_col: str = "Bezeichnung",
    feature_cols: T.List[str] = VOTE_COLS,
    other_cols: T.List = None,
    validate: bool = False,
) -> pd.DataFrame:
    "Reformats `df`"
    other_cols = ["date", "title"] if other_cols is None else other_cols
    tmp = df.loc[:, [id_col] + feature_cols + other_cols]
    tmp["issue"] = df["date"].dt.date.apply(str) + " " + df["title"]

    tmp = tmp.set_index([id_col, "issue"] + other_cols)
    tmp = (
        tmp[tmp == 1]
        .stack()
        .reset_index()
        .drop(labels=0, axis=1)
        .rename(columns={f"level_{2+len(other_cols)}": "vote"})
    )
    df = df.join(
        tmp.set_index(["Bezeichnung", "date", "title"]),
        on=["Bezeichnung", "date", "title"],
    ).drop(columns=VOTE_COLS)

    if validate:
        schemas.SHEET_SQUISHED(df)

    return df


DTYPES = {
    "Wahlperiode": int,
    "Sitzungnr": int,
    "Abstimmnr": int,
    "Fraktion/Gruppe": str,
    "Name": str,
    "Vorname": str,
    "Titel": str,
    "vote": str,
    "issue": str,
    #           'ja': bool, 'nein': bool, 'Enthaltung': bool, 'ungültig': bool, 'nichtabgegeben': bool, 'Bemerkung': str,
    "Bezeichnung": str,
    "sheet_name": str,
    "date": "datetime64[ns]",
    "title": str,
}


def set_sheet_dtypes(df: pd.DataFrame):
    for col, dtype in DTYPES.items():
        df[col] = df[col].astype(dtype)
    return df


def get_multiple_sheets_df(
    sheet_files: T.List[T.Union[Path, str]],
    file_title_maps: T.Dict[str, str] = None,
):
    "Loads, processes and concatenates multiple vote sheets"
    logger.info("Loading processing and concatenating multiple vote sheets")
    df = []
    for sheet_file in tqdm.tqdm(
        sheet_files, total=len(sheet_files), desc="Sheets"
    ):
        df.append(
            (
                get_sheet_df(sheet_file, file_title_maps=file_title_maps)
                .pipe(get_squished_dataframe)
                .pipe(set_sheet_dtypes)
            )
        )
    return pd.concat(df, ignore_index=True)


def get_multiple_sheets(
    html_path, sheet_path, nmax: int = None, dry: bool = False
):
    "Convenience function to perform downloading, storing, excel file detection and processing of votes to pd.DataFrame"

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = get_file_paths(html_path, pattern=RE_HTM)
    # extract excel sheet uris from htm files
    sheet_uris = collect_sheet_uris(html_file_paths)
    # download excel files
    download_multiple_sheets(
        sheet_uris, sheet_path=sheet_path, nmax=nmax, dry=dry
    )
    # locate downloaded excel files
    sheet_files = get_file_paths(sheet_path, pattern=RE_FNAME)
    # process excel files
    file_title_maps = get_file2poll_maps(sheet_uris, sheet_path)
    df = get_multiple_sheets_df(sheet_files, file_title_maps=file_title_maps)
    return df
