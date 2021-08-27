from bs4 import BeautifulSoup
from pathlib import Path
import re
import typing
import requests
import tqdm
import time
import pandas as pd
from fastcore.all import *
from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")
# default level for this module should be INFO

RE_HTM = re.compile("(\.html?)")
RE_FNAME = re.compile("(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")
VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]

# TODO: add dry mode for http requests
# TODO: add logger debug statements


def get_file_paths(path: typing.Union[Path, str], suffix: str = None, pattern=None):
    "Collecting files with a specific suffix or pattern from `path`"
    logger.info(f"Collecting using suffix = {suffix} and pattern = {pattern}")

    return (
        Path(path)
        .ls()
        .filter(
            lambda x: (suffix and x.suffix == suffix)
            or (pattern and pattern.search(str(x)))
        )
        .unique()
    )


def test_file_paths(html_file_paths: list, html_path: Path):
    logger.debug("Sanity checking the number of found files")
    assert len(html_file_paths) > 0
    assert len(html_file_paths) >= len(get_file_paths(html_path, suffix=".htm"))


def collect_sheet_uris(html_file_paths: typing.List[Path], pattern=None):
    "Extracting URIs to roll call votes stored in excel sheets"
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


def test_sheet_uris(sheet_uris):
    logger.debug("Sanity checking collected URIs to excel sheets")
    assert isinstance(sheet_uris, dict)
    assert (
        "10.09.2020: Abstrakte Normenkontrolle - Düngeverordnung (Beschlussempfehlung)"
        in sheet_uris
    )


def download_sheet(uri: str, sheet_path: typing.Union[Path, str], dry: bool = False):
    "Downloads a single excel sheet given `uri` and writes to `sheet_path`"
    sheet_path.mkdir(exist_ok=True)
    file = Path(sheet_path) / uri.split("/")[-1]
    logger.debug(f"Writing requesting excel sheet: {uri} and writing to {file}")
    if dry:
        return
    with open(file, "wb") as f:
        r = requests.get(uri)
        f.write(r.content)


def get_sheet_fname(uri):
    return uri.split("/")[-1]


def download_multiple_sheets(
    uris: typing.Dict[str, str],
    sheet_path: typing.Union[Path, str],
    t_sleep: float = 0.01,
    nmax: int = None,
    dry: bool = False,
):
    "Downloads multiple excel sheets containing roll call votes using `uris`, writing to `sheet_path`"
    n = min(nmax, len(uris)) if nmax else len(uris)
    logger.info(
        f"Downloading {n} excel sheets and storing under {sheet_path} (dry = {dry})"
    )
    known_sheets = get_file_paths(sheet_path, pattern=RE_FNAME)

    for i, (_, uri) in tqdm.tqdm(enumerate(uris.items()), desc="File", total=n):
        if nmax is not None and i > nmax:
            break
        fname = get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            continue

        download_sheet(uri, sheet_path=sheet_path, dry=dry)

        time.sleep(t_sleep)


def get_file2poll_maps(
    uris: typing.Dict[str, str], sheet_path: typing.Union[str, Path]
):
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = get_file_paths(sheet_path, pattern=RE_FNAME)
    file_poll_title_maps = {}
    for poll_title, uri in uris.items():

        fname = get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            file_poll_title_maps[fname] = poll_title
    return file_poll_title_maps


def test_file_title_maps(
    file_poll_title_maps: typing.Dict[str, str], uris: typing.Dict[str, str]
):
    assert len(file_poll_title_maps) <= len(uris)


def is_date(s: str, fun: typing.Callable):
    try:
        _ = fun(s)
        return True
    except:
        return False


def get_sheet_df(
    sheet_file: typing.Union[str, Path], file_title_maps: typing.Dict[str, str] = None
):
    "Parsing xlsx and xls files into dataframes"

    if Path(sheet_file).stat().st_size == 0:
        logger.warning(f"{sheet_file} is of size 0, skipping ...")
        return

    dfs = pd.read_excel(sheet_file, sheet_name=None)

    assert len(dfs) == 1, "The sheet file has more than one page, that's unexpected."

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

    return df.pipe(disambiguate_party)


def test_get_sheet_df(df: pd.DataFrame):
    assert isinstance(df, pd.DataFrame)
    assert all(
        [
            col in df.columns.values
            for col in [
                "Wahlperiode",
                "Sitzungnr",
                "Abstimmnr",
                "Fraktion/Gruppe",
                "Name",
                "Vorname",
                "Titel",
                "ja",
                "nein",
                "Enthaltung",
                "ungültig",
                "nichtabgegeben",
                "Bezeichnung",
                "sheet_name",
                "date",
                "title",
            ]
        ]
    )


def handle_title_and_date(full_title: str, sheet_file):
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
    return title, date


PARTY_MAP = {
    "BÜNDNIS`90/DIE GRÜNEN": "BÜ90/GR",
    "DIE LINKE": "DIE LINKE.",
    "fraktionslos": "Fraktionslos",
    "fraktionslose": "Fraktionslos",
}


def disambiguate_party(
    df: pd.DataFrame, col: str = "Fraktion/Gruppe", party_map: dict = None
):
    if party_map is None:
        party_map = PARTY_MAP
    df[col] = df[col].apply(lambda x: x if x not in party_map else party_map[x])
    return df


def get_squished_dataframe(
    df: pd.DataFrame,
    id_col: str = "Bezeichnung",
    feature_cols: typing.List[str] = VOTE_COLS,
    other_cols: typing.List = None,
):
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
    return df.join(
        tmp.set_index(["Bezeichnung", "date", "title"]),
        on=["Bezeichnung", "date", "title"],
    ).drop(columns=VOTE_COLS)


def test_squished_df(df_squished: pd.DataFrame, df: pd.DataFrame):
    assert len(df_squished) == len(df)
    assert "vote" in df_squished.columns
    assert "issue" in df_squished.columns
    assert not any([v in df_squished.columns for v in VOTE_COLS])


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
    sheet_files: typing.List[typing.Union[Path, str]],
    file_title_maps: typing.Dict[str, str] = None,
):
    "Loads, processes and concatenates multiple vote sheets"
    logger.info("Loading processing and concatenating multiple vote sheets")
    df = []
    for sheet_file in tqdm.tqdm(sheet_files, total=len(sheet_files), desc="Sheets"):
        df.append(
            (
                get_sheet_df(sheet_file, file_title_maps=file_title_maps)
                .pipe(get_squished_dataframe)
                .pipe(set_sheet_dtypes)
            )
        )
    return pd.concat(df, ignore_index=True)


def get_multiple_sheets(html_path, sheet_path, nmax: int = None, dry: bool = False):
    "Convenience function to perform downloading, storing, excel file detection and processing of votes to pd.DataFrame"

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = get_file_paths(html_path, pattern=RE_HTM)
    # extract excel sheet uris from htm files
    sheet_uris = collect_sheet_uris(html_file_paths)
    # download excel files
    download_multiple_sheets(sheet_uris, sheet_path=sheet_path, nmax=nmax, dry=dry)
    # locate downloaded excel files
    sheet_files = get_file_paths(sheet_path, pattern=RE_FNAME)
    # process excel files
    file_title_maps = get_file2poll_maps(sheet_uris, sheet_path)
    df = get_multiple_sheets_df(sheet_files, file_title_maps=file_title_maps)
    return df
