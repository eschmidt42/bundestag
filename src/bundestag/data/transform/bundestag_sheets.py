from pathlib import Path
from typing import Callable

import pandas as pd
import tqdm
import xlrd

import bundestag.data.utils as data_utils
import bundestag.logging as logging
import bundestag.schemas as schemas

logger = logging.logger

VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]


def get_file2poll_maps(uris: dict[str, str], sheet_path: str | Path) -> dict[str, str]:
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = data_utils.get_file_paths(sheet_path, pattern=data_utils.RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = data_utils.get_sheet_fname(uri)
        file = Path(sheet_path) / fname
        if file in known_sheets:
            file2poll[fname] = poll_title
    return file2poll


def is_date(s: str, fun: Callable):
    try:
        _ = fun(s)
        return True
    except:
        return False


def file_size_is_zero(file: Path):
    file_size = file.stat().st_size
    if file_size == 0:
        logger.warning(f"{file=} is of size 0, skipping ...")
        return True
    return False


def read_excel(file: Path, engine: str = "xlrd") -> pd.DataFrame:
    try:
        dfs = pd.read_excel(file, sheet_name=None, engine=engine)
    except ValueError as ex:
        dfs = pd.read_excel(file, sheet_name=None, engine="openpyxl")
    except xlrd.XLRDError as ex:
        # file is erroneous and needs to be skipped
        logger.error(f"{file=} is erroneous and is skipped: {ex}")
        return None
    return dfs


def get_sheet_df(
    sheet_file: str | Path,
    file_title_maps: dict[str, str] | None = None,
    validate: bool = False,
) -> pd.DataFrame | None:
    "Parsing xlsx and xls files into dataframes"

    sheet_file = Path(sheet_file)
    if file_size_is_zero(sheet_file):
        return

    dfs = read_excel(sheet_file)
    if dfs is None:
        return

    # sanity check that there is only one sheet
    if len(dfs) > 1:
        raise ValueError(f"{sheet_file=} has more than one page, that's unexpected.")

    # assigning sheet name to the dataframe
    for name, df in dfs.items():
        df["sheet_name"] = name

    # verifying that all vote columns (ja, nein, Enthaltung, ungültig, nichtabgegeben) are plausible
    mask = df.loc[:, VOTE_COLS].sum(axis=1) != 1
    if mask.any():
        logger.error(
            f"{sheet_file=} has rows with more than one vote:\n{df.loc[mask, :]}"
        )

    # assinging date and title columns
    if file_title_maps is not None:
        title, date = handle_title_and_date(
            file_title_maps[sheet_file.name], sheet_file
        )
    else:
        title, date = None, None

    df["date"] = date
    df["title"] = title

    df = df.pipe(disambiguate_party)

    if validate:
        schemas.SHEET.validate(df)

    return df


def handle_title_and_date(
    full_title: str, sheet_file: Path
) -> tuple[str, pd.DatetimeTZDtype]:
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
    df: pd.DataFrame, col: str = "Fraktion/Gruppe", party_map: dict | None = None
) -> pd.DataFrame:
    if party_map is None:
        party_map = PARTY_MAP
    df[col] = df[col].apply(lambda x: x if x not in party_map else party_map[x])
    return df


def get_squished_dataframe(
    df: pd.DataFrame,
    id_col: str = "Bezeichnung",
    feature_cols: list[str] = VOTE_COLS,
    other_cols: list | None = None,
    validate: bool = False,
) -> pd.DataFrame:
    "Reformats `df` by squishing the vote columns (ja, nein, Enthaltung, ...) into one column"
    other_cols = ["date", "title"] if other_cols is None else other_cols
    tmp = df.loc[:, [id_col] + feature_cols + other_cols]

    # add issue column
    tmp["issue"] = df["date"].dt.date.apply(str) + " " + df["title"]

    tmp = tmp.set_index([id_col, "issue"] + other_cols)
    tmp = (
        tmp[tmp == 1]
        .stack()
        .reset_index()
        .drop(labels=0, axis=1)
        .rename(columns={f"level_{2 + len(other_cols)}": "vote"})
    )
    df = df.join(
        tmp.set_index(["Bezeichnung", "date", "title"]),
        on=["Bezeichnung", "date", "title"],
    ).drop(columns=VOTE_COLS)

    if validate:
        schemas.SHEET_FINAL(df)

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


def set_sheet_dtypes(df: pd.DataFrame, dtypes: dict[str, str | object] | None = None):
    dtypes = DTYPES if dtypes is None else dtypes
    for col, dtype in dtypes.items():
        df[col] = df[col].astype(dtype)
    return df


def get_multiple_sheets_df(
    sheet_files: list[Path | str],
    file_title_maps: dict[str, str] | None = None,
    validate: bool = False,
):
    "Loads, processes and concatenates multiple vote sheets"
    logger.info("Loading processing and concatenating multiple vote sheets")
    df = []
    n_skipped = 0
    for sheet_file in tqdm.tqdm(sheet_files, total=len(sheet_files), desc="Sheets"):
        sheet_df = get_sheet_df(
            sheet_file, file_title_maps=file_title_maps, validate=validate
        )
        if sheet_df is None:
            n_skipped += 1
            continue
        df.append(
            (
                sheet_df.pipe(get_squished_dataframe, validate=validate).pipe(
                    set_sheet_dtypes
                )
            )
        )
    if n_skipped > 0:
        n = len(sheet_files)
        pct = n_skipped / n * 100.0
        logger.warning(
            f"{n_skipped} / {n} = {pct:.2f} % files skipped due to excel parsing error"
        )
    return pd.concat(df, ignore_index=True)


def run(
    html_path: Path,
    sheet_path: Path,
    preprocessed_path: Path,
    dry: bool = False,
    validate: bool = False,
):
    import bundestag.data.download.bundestag_sheets as download_sheets

    logger.info("Start parsing sheets")

    # ensuring path exists
    if not dry and not html_path.exists():
        raise ValueError(f"{html_path=} doesn't exist, terminating transformation.")
    if not dry and not sheet_path.exists():
        raise ValueError(f"{sheet_path=} doesn't exist, terminating transformation.")
    if not dry and not preprocessed_path.exists():
        data_utils.ensure_path_exists(preprocessed_path)

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = data_utils.get_file_paths(html_path, pattern=data_utils.RE_HTM)
    # extract excel sheet uris from htm files
    sheet_uris = download_sheets.collect_sheet_uris(html_file_paths)

    # locate downloaded excel files
    sheet_files = data_utils.get_file_paths(sheet_path, pattern=data_utils.RE_FNAME)
    # process excel files
    file_title_maps = get_file2poll_maps(sheet_uris, sheet_path)
    df = get_multiple_sheets_df(
        sheet_files, file_title_maps=file_title_maps, validate=validate
    )

    if not dry:
        path = preprocessed_path / "bundestag.de_votes.parquet"
        logger.info(f"Writing to {path}")
        df.to_parquet(path)

    logger.info("Done parsing sheets")
