import re
import typing as T
from pathlib import Path

import pandas as pd
import tqdm

import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger

RE_HTM = re.compile("(\.html?)")
RE_FNAME = re.compile("(\.xlsx?)")
RE_SHEET = re.compile("(XLSX?)")
VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]


def get_file2poll_maps(
    uris: T.Dict[str, str], sheet_path: T.Union[str, Path]
) -> T.Dict[str, str]:
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = data_utils.get_file_paths(sheet_path, pattern=RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = data_utils.get_sheet_fname(uri)
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
