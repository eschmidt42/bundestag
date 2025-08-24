from pathlib import Path

import pandas as pd
import tqdm
import xlrd

import bundestag.data.download.bundestag_sheets as download_sheets
import bundestag.logging as logging
import bundestag.schemas as schemas
from bundestag.data.utils import (
    RE_FNAME,
    RE_HTM,
    ensure_path_exists,
    get_file_paths,
    get_sheet_fname,
)

logger = logging.logger

VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]


def get_file2poll_maps(uris: dict[str, str], sheet_dir: Path) -> dict[str, str]:
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = get_file_paths(sheet_dir, pattern=RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = get_sheet_fname(uri)
        sheet_path = sheet_dir / fname
        if sheet_path in known_sheets:
            file2poll[fname] = poll_title
    return file2poll


def is_date(s: str, dayfirst: bool) -> bool:
    try:
        _ = pd.to_datetime(s, dayfirst=dayfirst)
    except:
        return False
    else:
        return True


def file_size_is_zero(file: Path) -> bool:
    file_size = file.stat().st_size
    if file_size == 0:
        logger.warning(f"{file=} is of size 0, skipping ...")
        return True
    return False


class ExcelReadException(Exception): ...


def read_excel(file: Path) -> pd.DataFrame:
    try:
        dfs = pd.read_excel(file, sheet_name=None, engine="openpyxl")
        # sanity check that there is only one sheet
        if len(dfs) > 1:
            raise ValueError(f"{file=} has more than one page, that's unexpected.")
        sheet_name, df = next(iter(dfs.items()))
        df["sheet_name"] = sheet_name
    except:
        try:
            df = pd.read_excel(file, engine="xlrd")
        except xlrd.biffh.XLRDError:
            raise ExcelReadException(f"Failed to parse {file}.")
    return df


def verify_vote_columns(sheet_file: Path, df: pd.DataFrame):
    "verifying that all vote columns (ja, nein, Enthaltung, ungültig, nichtabgegeben) are plausible"
    mask = df.loc[:, VOTE_COLS].sum(axis=1) != 1
    if mask.any():
        msg = f"{sheet_file=} has rows with more than one vote:\n{df.loc[mask, :]}"
        logger.error(msg)
        raise ValueError(msg)


def handle_title_and_date(
    full_title: str, sheet_file: Path
) -> tuple[str, pd.Timestamp | None]:
    "Extracting the title of the roll call vote and the date"

    title = full_title.split(":")

    date = title[0]

    if is_date(date, True):
        date = pd.to_datetime(date, dayfirst=True)
        title = ":".join(title[1:])
    elif is_date(sheet_file.name.split("_")[0], False):
        date = pd.to_datetime(sheet_file.name.split("_")[0])
        title = full_title
    else:
        date = None
        title = full_title

    title = title.strip()
    return title, date


def assign_date_and_title_columns(
    sheet_file: Path, df: pd.DataFrame, file_title_maps: dict[str, str] | None = None
) -> pd.DataFrame:
    if file_title_maps is not None:
        title, date = handle_title_and_date(
            file_title_maps[sheet_file.name], sheet_file
        )
    else:
        title, date = None, None

    df["date"] = date
    df["title"] = title
    return df


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


def get_sheet_df(
    sheet_file: str | Path,
    file_title_maps: dict[str, str] | None = None,
    validate: bool = False,
) -> pd.DataFrame:
    "Parsing xlsx and xls files into dataframes"

    sheet_file = Path(sheet_file)

    df = read_excel(sheet_file)

    verify_vote_columns(sheet_file, df)

    df = assign_date_and_title_columns(sheet_file, df, file_title_maps)

    df = df.pipe(disambiguate_party)

    if validate:
        schemas.SHEET.validate(df)

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
    df_sub = df.loc[:, [id_col] + feature_cols + other_cols]

    # add issue column
    mask_date_title_missing = df_sub["date"].isnull() | df_sub["title"].isnull()

    n_missing = mask_date_title_missing.sum()
    if n_missing > 0:  # TODO: that this can happen is weird - error in processing?
        raise ValueError(
            f"Missing date and or title information for {n_missing:_} rows ({n_missing / len(df_sub):.2%}), did you provide file_title_maps upstream?"
        )

    df_sub["issue"] = df_sub["date"].dt.date.apply(str) + " " + df["title"]

    df_sub = df_sub.set_index([id_col, "issue"] + other_cols)
    df_sub = (
        df_sub[df_sub == 1]
        .stack()
        .reset_index()
        .drop(labels=0, axis=1)
        .rename(columns={f"level_{2 + len(other_cols)}": "vote"})
    )
    df = df.join(
        df_sub.set_index(["Bezeichnung", "date", "title"]),
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
        df[col] = df[col].astype(dtype)  # type: ignore
    return df


def get_multiple_sheets_df(
    sheet_files: list[Path],
    file_title_maps: dict[str, str] | None = None,
    validate: bool = False,
):
    "Loads, processes and concatenates multiple vote sheets"
    logger.info("Loading processing and concatenating multiple vote sheets")
    df = []
    n_empty = 0
    n_errors = 0
    for sheet_file in tqdm.tqdm(sheet_files, total=len(sheet_files), desc="Sheets"):
        if file_size_is_zero(sheet_file):
            n_empty += 1
            continue
        try:
            sheet_df = get_sheet_df(
                sheet_file, file_title_maps=file_title_maps, validate=validate
            )
        except ExcelReadException:
            n_errors += 1
            continue

        sheet_df = get_squished_dataframe(sheet_df, validate=validate)
        sheet_df = set_sheet_dtypes(sheet_df)

        df.append(sheet_df)

    n = len(sheet_files)
    if n_empty > 0:
        logger.warning(f"{n_empty:_} / {n:_} = {n_empty / n:.2%} % files were empty.")
    if n_errors > 0:
        logger.warning(
            f"{n_errors:_} / {n:_} = {n_errors / n:.2%} % files skipped due to some excel parsing error, likely xls files."
        )
    return pd.concat(df, ignore_index=True)


def run(
    html_path: Path,
    sheet_path: Path,
    preprocessed_path: Path,
    dry: bool = False,
    validate: bool = False,
    assume_yes: bool = False,
):
    logger.info("Start parsing sheets")

    # ensuring path exists
    if not dry and not html_path.exists():
        raise ValueError(f"{html_path=} doesn't exist, terminating transformation.")
    if not dry and not sheet_path.exists():
        raise ValueError(f"{sheet_path=} doesn't exist, terminating transformation.")
    if not dry and not preprocessed_path.exists():
        ensure_path_exists(preprocessed_path, assume_yes=assume_yes)

    html_path, sheet_path = Path(html_path), Path(sheet_path)
    # collect htm files
    html_file_paths = get_file_paths(html_path, pattern=RE_HTM)
    # extract excel sheet uris from htm files
    sheet_uris = download_sheets.collect_sheet_uris(html_file_paths)

    # locate downloaded excel files
    sheet_files = get_file_paths(sheet_path, pattern=RE_FNAME)
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
