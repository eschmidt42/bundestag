import datetime
import json
import logging
from pathlib import Path
from time import perf_counter

import pandas as pd
import polars as pl
import tqdm
import xlrd

from bundestag.data.download.bundestag_sheets import Source, collect_sheet_uris
from bundestag.data.utils import (
    RE_FNAME,
    RE_HTM,
    ensure_path_exists,
    file_size_is_zero,
    get_file_paths,
    get_sheet_filename,
)

logger = logging.getLogger(__name__)

VOTE_COLS = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]


def get_file2poll_maps(uris: dict[str, str], sheet_dir: Path) -> dict[str, str]:
    "Creates a file name (so file needs to exist) to poll title map"
    known_sheets = get_file_paths(sheet_dir, pattern=RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = get_sheet_filename(uri)
        sheet_path = sheet_dir / fname
        if sheet_path in known_sheets:
            file2poll[fname] = poll_title
    return file2poll


def parse_date(date: str, dayfirst: bool) -> tuple[datetime.date | None, str]:
    if dayfirst:
        formats = ["%d.%m.%Y", "%d%m%y"]
    else:
        formats = ["%Y-%m-%d", "%Y%m%d"]

    parsed_date = None
    matched_format = ""
    for f in formats:
        try:
            parsed_date = datetime.datetime.strptime(date, f)
            matched_format = f
            break
        except:
            pass

    return parsed_date, matched_format


def is_date(s: str, dayfirst: bool) -> bool:
    parsed_date, _ = parse_date(s, dayfirst=dayfirst)
    if parsed_date is not None:
        return True
    return False


class ExcelReadException(Exception): ...


SHEET_SCHEMA_READ_EXCEL = pl.Schema(
    {
        "Wahlperiode": pl.Int64(),
        "Sitzungnr": pl.Int64(),
        "Abstimmnr": pl.Int64(),
        "Fraktion/Gruppe": pl.String(),
        "AbgNr": pl.Int64(),
        "Name": pl.String(),
        "Vorname": pl.String(),
        "Titel": pl.String(),
        "ja": pl.Int64(),
        "nein": pl.Int64(),
        "Enthaltung": pl.Int64(),
        "ungültig": pl.Int64(),
        "nichtabgegeben": pl.Int64(),
        "Bezeichnung": pl.String(),
        "Bemerkung": pl.String(),
        "sheet_name": pl.String(),
    }
)


def read_excel(file: Path) -> pl.DataFrame:
    try:
        dfs = pl.read_excel(file, sheet_name=None, engine="openpyxl")
        # sanity check that there is only one sheet
        if isinstance(dfs, dict):
            if len(dfs) > 1:
                raise ValueError(f"{file=} has more than one page, that's unexpected.")

            sheet_name, df = next(iter(dfs.items()))
            df = df.with_columns(**{"sheet_name": pl.lit(sheet_name)})

        else:
            df = dfs.with_columns(**{"sheet_name": pl.lit("")})

    except:
        try:
            df = pd.read_excel(file, engine="xlrd")

        except xlrd.biffh.XLRDError:
            raise ExcelReadException(f"Failed to parse {file}.")

        df = pl.from_pandas(df)
        df = df.with_columns(**{"sheet_name": pl.lit("")})

    for c in ["AbgNr", "Bemerkung"]:
        if c not in df.columns:
            df = df.with_columns(**{c: None})

    df = pl.from_dict(df.to_dict(), schema=SHEET_SCHEMA_READ_EXCEL)
    return df


def verify_vote_columns(sheet_file: Path, df: pl.DataFrame):
    "verifying that all vote columns (ja, nein, Enthaltung, ungültig, nichtabgegeben) are plausible"
    tmp = df.with_columns(
        **{"hits": df.select(VOTE_COLS).sum_horizontal()}
    ).with_columns(**{"implausible": pl.col("hits").ne(pl.lit(1))})

    if tmp["implausible"].any():
        msg = f"{sheet_file=} has rows with more than one vote:\n{tmp.filter(pl.col('implausible'))}"
        logger.error(msg)
        raise ValueError(msg)


def handle_title_and_date(
    full_title: str, sheet_file: Path
) -> tuple[str, datetime.date | None]:
    "Extracting the title of the roll call vote and the date from full_title, if possible, otherwise fall back to sheet_file."

    # get date from full_title
    title_clean = full_title.strip()
    title = title_clean.split(":")
    date_in_title = title[0]
    title_has_date = is_date(date_in_title, dayfirst=True)

    # get date from file name
    date_in_fname = sheet_file.name.split("_")[0]
    fname_has_date = is_date(date_in_fname, dayfirst=False)

    if title_has_date:
        date, _ = parse_date(date_in_title, dayfirst=True)
        title = ":".join(title[1:]).strip()
        return title, date
    elif fname_has_date:
        date, _ = parse_date(date_in_fname, dayfirst=False)
        return title_clean, date
    else:
        return title_clean, None


def assign_date_and_title_columns(
    sheet_file: Path, df: pl.DataFrame, file_title_maps: dict[str, str] | None = None
) -> pl.DataFrame:
    if file_title_maps is not None and sheet_file.name in file_title_maps:
        title, date = handle_title_and_date(
            file_title_maps[sheet_file.name], sheet_file
        )
    else:
        title, date = None, None

    df = df.with_columns(
        **{
            "date": pl.lit(date),
            "title": pl.lit(title),
        }
    )

    return df


PARTY_MAP = {
    "BÜNDNIS`90/DIE GRÜNEN": "BÜ90/GR",
    "DIE LINKE": "DIE LINKE.",
    "fraktionslos": "Fraktionslos",
    "fraktionslose": "Fraktionslos",
}


def disambiguate_party(
    df: pl.DataFrame, col: str = "Fraktion/Gruppe", party_map: dict | None = None
) -> pl.DataFrame:
    if party_map is None:
        party_map = PARTY_MAP
    df = df.with_columns(
        **{
            col: pl.col(col).map_elements(
                lambda x: x if x not in party_map else party_map[x],
                return_dtype=pl.String,
            )
        }
    )

    return df


SHEET_SCHEMA_GET_SHEET_DF = pl.Schema(
    {
        "Wahlperiode": pl.Int64(),
        "Sitzungnr": pl.Int64(),
        "Abstimmnr": pl.Int64(),
        "Fraktion/Gruppe": pl.String(),
        "AbgNr": pl.Int64(),
        "Name": pl.String(),
        "Vorname": pl.String(),
        "Titel": pl.String(),
        "ja": pl.Int64(),
        "nein": pl.Int64(),
        "Enthaltung": pl.Int64(),
        "ungültig": pl.Int64(),
        "nichtabgegeben": pl.Int64(),
        "Bezeichnung": pl.String(),
        "Bemerkung": pl.String(),
        "sheet_name": pl.String(),
        "date": pl.Datetime(time_unit="ns"),
        "title": pl.String(),
    }
)


def get_sheet_df(
    sheet_file: str | Path,
    file_title_maps: dict[str, str] | None = None,
) -> pl.DataFrame:
    "Parsing xlsx and xls files into dataframes"

    sheet_file = Path(sheet_file)

    df = read_excel(sheet_file)

    verify_vote_columns(sheet_file, df)

    df = assign_date_and_title_columns(sheet_file, df, file_title_maps)

    df = disambiguate_party(df)

    df = pl.from_dict(df.to_dict(), schema=SHEET_SCHEMA_GET_SHEET_DF)

    return df


def create_vote_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Transforms the vote columns (ja, nein, etc.) into a single 'vote' column
    by unpivoting the dataframe. It handles cases where a vote is cast (one of
    the columns is 1) and where no vote is cast (all columns are 0), labeling
    the latter as 'error'.
    """
    # Keep original columns to join back later
    original_cols = df.columns

    # Add a unique row identifier to pivot and join correctly
    df = df.with_row_index("__row_nr__")

    # Unpivot the DataFrame to long format
    unpivoted = df.unpivot(
        index=["__row_nr__"], on=VOTE_COLS, variable_name="vote", value_name="value"
    )

    # Filter for the cast votes (where value is 1)
    votes = unpivoted.filter(pl.col("value") == 1).select(["__row_nr__", "vote"])

    # Join the vote information back to the original DataFrame
    df = df.join(votes, on="__row_nr__", how="left")

    # For rows where no vote was cast, the 'vote' column will be null.
    # We fill these with 'error'.
    df = df.with_columns(vote=pl.col("vote").fill_null("error"))

    # Return the DataFrame with the original columns plus the new 'vote' column
    return df.select(original_cols + ["vote"])


SHEET_SCHEMA_GET_SQUISHED_DATAFRAME = pl.Schema(
    {
        "Wahlperiode": pl.Int64(),
        "Sitzungnr": pl.Int64(),
        "Abstimmnr": pl.Int64(),
        "Fraktion/Gruppe": pl.String(),
        "AbgNr": pl.Int64(),
        "Name": pl.String(),
        "Vorname": pl.String(),
        "Titel": pl.String(),
        "Bezeichnung": pl.String(),
        "Bemerkung": pl.String(),
        "sheet_name": pl.String(),
        "date": pl.Datetime(time_unit="ns"),
        "title": pl.String(),
        "issue": pl.String(),
        "vote": pl.String(),
    }
)


def get_squished_dataframe(
    df: pl.DataFrame,
    id_col: str = "Bezeichnung",
    feature_cols: list[str] = VOTE_COLS,
    other_cols: list | None = None,
    validate: bool = False,
) -> pl.DataFrame:
    "Reformats `df` by squishing the vote columns (ja, nein, Enthaltung, ...) into one column"

    other_cols = ["date", "title"] if other_cols is None else other_cols
    df_sub = df.select([id_col] + feature_cols + other_cols)

    # add issue column
    df_sub = df_sub.with_columns(
        **{
            "date or title missing": pl.col("date").is_null()
            | pl.col("title").is_null()
        }
    )

    n_missing = df_sub["date or title missing"].sum()

    if n_missing > 0:  # TODO: that this can happen is weird - error in processing?
        raise ValueError(
            f"Missing date and or title information for {n_missing:_} rows ({n_missing / len(df_sub):.2%}), did you provide file_title_maps upstream?"
        )

    df_sub = df_sub.drop("date or title missing")

    df_sub = df_sub.with_columns(
        **{"issue": pl.col("date").dt.date().cast(pl.String) + " " + pl.col("title")}
    )

    df_sub = create_vote_column(df_sub)

    df = df.drop(VOTE_COLS).join(
        df_sub.drop(VOTE_COLS), on=["Bezeichnung", "date", "title"]
    )
    df = pl.from_dict(df.to_dict(), schema=SHEET_SCHEMA_GET_SQUISHED_DATAFRAME)

    return df


def get_multiple_sheets_df(
    sheet_files: list[Path],
    file_title_maps: dict[str, str],
    validate: bool = False,
) -> pl.DataFrame:
    "Loads, processes and concatenates multiple vote sheets"
    logger.info("Loading processing and concatenating multiple vote sheets")
    df = []
    n_empty = 0
    n_errors = 0
    for sheet_file in tqdm.tqdm(sheet_files, total=len(sheet_files), desc="Sheets"):
        if file_size_is_zero(sheet_file):
            n_empty += 1
            continue
        if sheet_file.name not in file_title_maps:
            continue
        try:
            sheet_df = get_sheet_df(sheet_file, file_title_maps=file_title_maps)
        except ExcelReadException:
            n_errors += 1
            continue

        try:
            sheet_df = get_squished_dataframe(sheet_df, validate=validate)
        except ValueError as ex:
            raise ValueError(f"Parsing failed for {sheet_file} with ValueError: {ex}")

        df.append(sheet_df)

    n = len(sheet_files)
    if n_empty > 0:
        logger.warning(f"{n_empty:_} / {n:_} = {n_empty / n:.2%} % files were empty.")
    if n_errors > 0:
        logger.warning(
            f"{n_errors:_} / {n:_} = {n_errors / n:.2%} % files skipped due to some excel parsing error, likely xls files."
        )
    return pl.concat(df)


def get_sheet_uris_from_json(source: Source, json_path: Path) -> dict[str, str]:
    if not json_path.exists():
        raise ValueError(
            f"Running this function with {source=} requires that {json_path=} exists."
        )
    with json_path.open("r") as f:
        sheet_uris = json.load(f)
    return sheet_uris


def run(
    html_dir: Path,
    sheet_dir: Path,
    preprocessed_path: Path,
    dry: bool = False,
    validate: bool = False,
    assume_yes: bool = False,
    source: Source = Source.html_file,
    json_filename: str = "xlsx_uris.json",
):
    logger.info("Start parsing sheets")
    start_time = perf_counter()
    # ensuring path exists
    if not dry and not html_dir.exists():
        raise ValueError(f"{html_dir=} doesn't exist, terminating transformation.")
    if not dry and not sheet_dir.exists():
        raise ValueError(f"{sheet_dir=} doesn't exist, terminating transformation.")
    if not dry and not preprocessed_path.exists():
        ensure_path_exists(preprocessed_path, assume_yes=assume_yes)

    html_dir, sheet_dir = Path(html_dir), Path(sheet_dir)

    match source:
        case Source.json_file:
            json_path = html_dir.parent / json_filename

            sheet_uris = get_sheet_uris_from_json(source, json_path)

        case Source.html_file:
            # collect htm files
            html_file_paths = get_file_paths(html_dir, pattern=RE_HTM)
            # extract excel sheet uris from htm files
            sheet_uris = collect_sheet_uris(html_file_paths)

    # locate downloaded excel files
    sheet_files = get_file_paths(sheet_dir, pattern=RE_FNAME)
    # process excel files
    file_title_maps = get_file2poll_maps(sheet_uris, sheet_dir)
    df = get_multiple_sheets_df(
        sheet_files, file_title_maps=file_title_maps, validate=validate
    )

    if not dry:
        path = preprocessed_path / "bundestag.de_votes.parquet"
        logger.info(f"Writing to {path}")
        df.write_parquet(path)

    dt = str(perf_counter() - start_time)
    logger.info(f"Done parsing sheets after {dt}")
