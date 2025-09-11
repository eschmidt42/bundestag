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
    """Creates a mapping from a local file name to a poll title.

    This function iterates through a dictionary of poll titles and their corresponding URIs.
    For each URI, it generates the expected local filename and checks if the file exists in `sheet_dir`.
    If the file exists, it adds an entry to the map.

    Args:
        uris (dict[str, str]): A dictionary mapping poll titles to their download URIs.
        sheet_dir (Path): The directory where the downloaded sheets are stored.

    Returns:
        dict[str, str]: A dictionary mapping existing local file names to their poll titles.
    """

    known_sheets = get_file_paths(sheet_dir, pattern=RE_FNAME)
    file2poll = {}
    for poll_title, uri in uris.items():
        fname = get_sheet_filename(uri)
        sheet_path = sheet_dir / fname
        if sheet_path in known_sheets:
            file2poll[fname] = poll_title
    return file2poll


def parse_date(date: str, dayfirst: bool) -> tuple[datetime.date | None, str]:
    """Parses a date string into a datetime object.

    This function tries to parse a date string using a list of predefined formats.
    The formats depend on whether the day or the year is expected to be first.

    Args:
        date (str): The date string to parse.
        dayfirst (bool): If True, assumes formats like 'dd.mm.yyyy'. Otherwise, assumes 'yyyy-mm-dd'.

    Returns:
        tuple[datetime.date | None, str]: A tuple containing the parsed datetime object (or None if parsing fails)
                                           and the format string that was successfully used.
    """
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
    """Checks if a string can be parsed as a date.

    Args:
        s (str): The string to check.
        dayfirst (bool): If True, assumes formats like 'dd.mm.yyyy'. Otherwise, assumes 'yyyy-mm-dd'.

    Returns:
        bool: True if the string can be parsed as a date, False otherwise.
    """
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
    """Reads an Excel file into a Polars DataFrame, trying different engines.

    This function first attempts to read the Excel file using the 'openpyxl' engine.
    If that fails, it falls back to the 'xlrd' engine. It also handles cases where
    the file has multiple sheets (raising an error) and ensures that certain optional
    columns ('AbgNr', 'Bemerkung') exist in the final DataFrame.

    Args:
        file (Path): The path to the Excel file.

    Raises:
        ValueError: If the Excel file contains more than one sheet.
        ExcelReadException: If both 'openpyxl' and 'xlrd' engines fail to read the file.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the data from the Excel sheet.
    """
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
    """Verifies that the vote columns in the DataFrame are plausible.

    This function checks that for each row, exactly one of the vote columns
    ('ja', 'nein', 'Enthaltung', 'ungültig', 'nichtabgegeben') has a value of 1.
    If any row has a sum of these columns not equal to 1, it indicates an implausible vote.

    Args:
        sheet_file (Path): The path of the sheet file being verified, for logging purposes.
        df (pl.DataFrame): The DataFrame containing the vote data.

    Raises:
        ValueError: If any row has an implausible vote count.
    """

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
    """Extracts the title of the roll call vote and the date from a full title string.

    It first attempts to parse a date from the beginning of the `full_title`.
    If successful, the rest of the string is considered the title.
    If not, it tries to parse a date from the `sheet_file` name.
    If neither works, it returns the original title and None for the date.

    Args:
        full_title (str): The full title string, which may contain a date.
        sheet_file (Path): The path to the sheet file, used as a fallback for date extraction.

    Returns:
        tuple[str, datetime.date | None]: A tuple containing the extracted title and the date (or None).
    """

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
    """Assigns 'date' and 'title' columns to a DataFrame based on the sheet file.

    This function uses a mapping from file names to poll titles to find the full title for the given `sheet_file`.
    It then calls `handle_title_and_date` to extract the clean title and date, and adds them as new columns
    to the DataFrame.

    Args:
        sheet_file (Path): The path to the sheet file being processed.
        df (pl.DataFrame): The DataFrame to which the columns will be added.
        file_title_maps (dict[str, str] | None, optional): A mapping from file names to full poll titles. Defaults to None.

    Returns:
        pl.DataFrame: The DataFrame with 'date' and 'title' columns added.
    """
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
    """Disambiguates party names in a DataFrame column.

    This function maps certain party names to a standardized form using a provided dictionary.
    For example, it can map "BÜNDNIS`90/DIE GRÜNEN" to "BÜ90/GR".

    Args:
        df (pl.DataFrame): The DataFrame containing the party names.
        col (str, optional): The name of the column with party names. Defaults to "Fraktion/Gruppe".
        party_map (dict | None, optional): A dictionary for mapping party names. If None, a default map is used.

    Returns:
        pl.DataFrame: The DataFrame with the disambiguated party names.
    """
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
    """Parses a single Excel sheet file into a processed Polars DataFrame.

    This function orchestrates the reading and processing of a single sheet file, including:
    1. Reading the Excel file using `read_excel`.
    2. Verifying the integrity of the vote columns.
    3. Assigning date and title columns.
    4. Disambiguating party names.
    5. Ensuring the final DataFrame conforms to a specific schema.

    Args:
        sheet_file (str | Path): The path to the Excel sheet file.
        file_title_maps (dict[str, str] | None, optional): A mapping from file names to full poll titles. Defaults to None.

    Returns:
        pl.DataFrame: The processed Polars DataFrame.
    """

    sheet_file = Path(sheet_file)

    df = read_excel(sheet_file)

    verify_vote_columns(sheet_file, df)

    df = assign_date_and_title_columns(sheet_file, df, file_title_maps)

    df = disambiguate_party(df)

    df = pl.from_dict(df.to_dict(), schema=SHEET_SCHEMA_GET_SHEET_DF)

    return df


def create_vote_column(df: pl.DataFrame) -> pl.DataFrame:
    """Transforms wide-format vote columns into a single 'vote' column.

    This function takes a DataFrame with separate columns for each vote type (e.g., 'ja', 'nein')
    and unpivots it to create a single 'vote' column that contains the type of vote cast for each row.
    Rows where no vote was cast (all vote columns are 0) are marked as 'error'.

    Args:
        df (pl.DataFrame): The input DataFrame with vote columns in wide format.

    Returns:
        pl.DataFrame: The transformed DataFrame with a single 'vote' column.
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
    """Reformats a DataFrame by squishing the vote columns into a single 'vote' column.

    This function also creates a new 'issue' column by combining the date and title of the vote.
    It's a key step in transforming the data from a wide format to a long format that's easier to analyze.

    Args:
        df (pl.DataFrame): The input DataFrame.
        id_col (str, optional): The identifier column. Defaults to "Bezeichnung".
        feature_cols (list[str], optional): The list of vote columns to be squished. Defaults to VOTE_COLS.
        other_cols (list | None, optional): Other columns to include in the transformation. Defaults to None.
        validate (bool, optional): A flag for validation (currently unused). Defaults to False.

    Raises:
        ValueError: If 'date' or 'title' information is missing for any row.

    Returns:
        pl.DataFrame: The reformatted DataFrame with a single 'vote' column and an 'issue' column.
    """

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
    """Loads, processes, and concatenates multiple vote sheet files into a single DataFrame.

    This function iterates through a list of sheet files, processes each one using `get_sheet_df`
    and `get_squished_dataframe`, and then concatenates the results. It handles empty files and
    files that cause parsing errors by skipping them and logging a warning.

    Args:
        sheet_files (list[Path]): A list of paths to the Excel sheet files.
        file_title_maps (dict[str, str]): A mapping from file names to full poll titles.
        validate (bool, optional): A flag for validation passed down to `get_squished_dataframe`. Defaults to False.

    Raises:
        ValueError: If parsing fails for a specific file with a `ValueError`.

    Returns:
        pl.DataFrame: A single DataFrame containing the data from all processed sheets.
    """

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
    """Loads sheet URIs from a JSON file.

    Args:
        source (Source): The source type, used for raising a more informative error.
        json_path (Path): The path to the JSON file containing the sheet URIs.

    Raises:
        ValueError: If the specified JSON file does not exist.

    Returns:
        dict[str, str]: A dictionary of sheet URIs loaded from the JSON file.
    """
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
    """Main function to run the Bundestag sheet parsing and transformation pipeline.

        This function orchestrates the entire process of turning raw Excel sheets into a
        single, clean Parquet file. It can source the URIs for the sheets from either
    al HTML files or a pre-existing JSON file.

        Args:
            html_dir (Path): The directory containing HTML files (if source is 'html_file') or the parent of the JSON file.
            sheet_dir (Path): The directory where the raw Excel sheets are stored.
            preprocessed_path (Path): The directory where the final Parquet file will be saved.
            dry (bool, optional): If True, performs a dry run without writing any files. Defaults to False.
            validate (bool, optional): A flag for validation during processing. Defaults to False.
            assume_yes (bool, optional): If True, automatically creates the preprocessed path if it doesn't exist. Defaults to False.
            source (Source, optional): The source of the sheet URIs ('html_file' or 'json_file'). Defaults to Source.html_file.
            json_filename (str, optional): The name of the JSON file with URIs. Defaults to "xlsx_uris.json".

        Raises:
            ValueError: If required directories do not exist, or if the JSON source file is missing.
    """
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
