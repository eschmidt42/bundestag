import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import polars as pl
import pytest

from bundestag.data.transform.bundestag_sheets import (
    PARTY_MAP,
    ExcelReadException,
    assign_date_and_title_columns,
    create_vote_column,
    disambiguate_party,
    get_file2poll_maps,
    get_multiple_sheets_df,
    get_sheet_df,
    get_squished_dataframe,
    handle_title_and_date,
    is_date,
    parse_date,
    read_excel,
    run,
    verify_vote_columns,
)
from bundestag.data.utils import file_size_is_zero


def test_get_file2poll_maps():
    uris = {
        "title1": "uri/20201126_3_xls-data.xlsx",
        "title2": "uri/2.xlsx",
    }
    sheet_dir = Path("tests/data_for_testing")

    # line to test
    file2poll_map = get_file2poll_maps(uris, sheet_dir)

    assert len(file2poll_map) == 1
    assert "20201126_3_xls-data.xlsx" in file2poll_map
    assert file2poll_map["20201126_3_xls-data.xlsx"] == "title1"


@pytest.mark.parametrize(
    "date_str, dayfirst, expected_date, expected_format",
    [
        ("31.01.2022", True, datetime.datetime(2022, 1, 31), "%d.%m.%Y"),
        ("2022-01-31", True, None, ""),
        ("2022-01-31", False, datetime.datetime(2022, 1, 31), "%Y-%m-%d"),
        ("20220131", False, datetime.datetime(2022, 1, 31), "%Y%m%d"),
        ("220131", False, datetime.datetime(2022, 1, 31), "%y%m%d"),
        ("31.01.2022", False, None, ""),
        ("not a date", True, None, ""),
        ("not a date", False, None, ""),
    ],
)
def test_parse_date(
    date_str: str,
    dayfirst: bool,
    expected_date: datetime.datetime | None,
    expected_format: str,
):
    parsed_date, matched_format = parse_date(date_str, dayfirst)
    if date_str == "220131":
        with pytest.raises(AssertionError):
            assert parsed_date == expected_date
            assert matched_format == expected_format
        pytest.xfail(
            "datetime.datetime.strptime weirdly parses 220131 successfully with %Y%m%d."
        )
    else:
        assert parsed_date == expected_date
        assert matched_format == expected_format


@pytest.mark.parametrize(
    "date_str, dayfirst, expected",
    [
        ("31.01.2022", True, True),
        ("2022-01-31", True, False),
        ("2022-01-31", False, True),
        ("20220131", False, True),
        ("31.01.2022", False, False),
        ("not a date", True, False),
        ("not a date", False, False),
        ("123", False, False),
    ],
)
def test_is_date(date_str: str, dayfirst: bool, expected: bool):
    assert is_date(date_str, dayfirst) == expected


@pytest.mark.parametrize(
    "is_zero",
    [True, False],
)
def test_file_size_is_zero(tmp_path: Path, is_zero: bool):
    path = tmp_path / "some-random-file"
    path.touch()
    if not is_zero:
        with path.open("w") as f:
            f.write("blub")
    assert file_size_is_zero(path) is is_zero


# ========================= read_excel =========================


def test_read_excel_xls_fail():
    file_path = Path(
        "tests/data_for_testing/raw/bundestag/sheets/20140625_2_xls-data.xls"
    )
    with pytest.raises(ExcelReadException):
        _ = read_excel(file_path)


def test_read_excel_xlsx():
    file_path = Path(
        "tests/data_for_testing/raw/bundestag/sheets/20201126_3_xls-data.xlsx"
    )

    res = read_excel(file_path)
    assert res is not None
    assert "sheet_name" in res.columns


def test_read_excel_schema_mismatch(create_dummy_excel: Callable):
    """Tests that read_excel raises a SchemaError if the data does not match the expected schema."""
    # Create data with a type mismatch: 'Wahlperiode' should be integer, but we provide a string.
    invalid_data = {
        "Wahlperiode": ["nineteen"],  # This should be an integer
        "Sitzungnr": [195],
        "Abstimmnr": [3],
        "Fraktion/Gruppe": ["CDU/CSU"],
        "Name": ["Abercron"],
        "Vorname": ["Michael"],
        "Titel": ["von"],
        "ja": [1],
        "nein": [0],
        "Enthaltung": [0],
        "ungültig": [0],
        "nichtabgegeben": [0],
        "Bezeichnung": ["Dr. Michael von Abercron"],
        "Bemerkung": [None],
    }
    file_path = create_dummy_excel("invalid_schema.xlsx", invalid_data)

    with pytest.raises(pl.exceptions.InvalidOperationError):
        read_excel(file_path)


# ========================= verify_vote_columns =========================


def test_verify_vote_columns_valid():
    """Tests that verify_vote_columns does not raise an error for a valid DataFrame."""
    data = {
        "ja": [1, 0, 0],
        "nein": [0, 1, 0],
        "Enthaltung": [0, 0, 1],
        "ungültig": [0, 0, 0],
        "nichtabgegeben": [0, 0, 0],
    }
    df = pl.from_dict(data)
    sheet_file = Path("dummy_sheet.xlsx")

    try:
        verify_vote_columns(sheet_file, df)
    except ValueError:
        pytest.fail("verify_vote_columns raised ValueError unexpectedly!")


def test_verify_vote_columns_invalid_sum_greater_than_1():
    """Tests that verify_vote_columns raises a ValueError when a row has more than one vote."""
    data = {
        "ja": [1, 1, 0],  # second row is invalid
        "nein": [0, 1, 0],
        "Enthaltung": [0, 0, 1],
        "ungültig": [0, 0, 0],
        "nichtabgegeben": [0, 0, 0],
    }
    df = pl.from_dict(data)
    sheet_file = Path("dummy_sheet.xlsx")

    with pytest.raises(ValueError):
        verify_vote_columns(sheet_file, df)


def test_verify_vote_columns_invalid_sum_is_zero():
    """Tests that verify_vote_columns raises a ValueError when a row has no vote."""
    data = {
        "ja": [1, 0, 0],
        "nein": [0, 0, 0],
        "Enthaltung": [0, 0, 1],
        "ungültig": [0, 0, 0],
        "nichtabgegeben": [0, 0, 0],  # second row is invalid
    }
    df = pl.from_dict(data)
    sheet_file = Path("dummy_sheet.xlsx")

    with pytest.raises(ValueError):
        verify_vote_columns(sheet_file, df)


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({"col1": [1, 2, 3]})


# ========================= assign_date_and_title_columns =========================


def test_assign_date_and_title_columns_with_file_title_maps(sample_df: pl.DataFrame):
    sheet_file = Path("20230101_some_vote.xlsx")
    file_title_maps = {sheet_file.name: "01.01.2023: Some Vote Title"}
    df = assign_date_and_title_columns(sheet_file, sample_df, file_title_maps)
    assert "date" in df.columns
    assert "title" in df.columns
    assert df["date"].first() == datetime.datetime(2023, 1, 1)
    assert df["title"].first() == "Some Vote Title"


def test_assign_date_and_title_columns_with_date_in_filename(sample_df: pl.DataFrame):
    sheet_file = Path("20230101_some_vote.xlsx")
    file_title_maps = {sheet_file.name: "Some Vote Title Without Date"}
    df = assign_date_and_title_columns(sheet_file, sample_df, file_title_maps)
    assert "date" in df.columns
    assert "title" in df.columns
    assert df["date"].first() == datetime.datetime(2023, 1, 1)
    assert df["title"].first() == "Some Vote Title Without Date"


def test_assign_date_and_title_columns_no_date(sample_df: pl.DataFrame):
    sheet_file = Path("some_vote.xlsx")
    file_title_maps = {sheet_file.name: "Some Vote Title Without Date"}
    df = assign_date_and_title_columns(sheet_file, sample_df, file_title_maps)
    assert "date" in df.columns
    assert "title" in df.columns
    assert df["date"].first() is None
    assert df["title"].first() == "Some Vote Title Without Date"


def test_assign_date_and_title_columns_no_file_title_maps(sample_df: pl.DataFrame):
    sheet_file = Path("20230101_some_vote.xlsx")
    df = assign_date_and_title_columns(sheet_file, sample_df, None)
    assert "date" in df.columns
    assert "title" in df.columns
    assert df["date"].first() is None
    assert df["title"].first() is None


# ========================= handle_title_and_date =========================


def test_handle_title_and_date_with_date_in_title():
    """
    Tests extraction when the date is present at the beginning of the title string.
    """
    full_title = "26.11.2020: Entschließungsantrag zu dem Antrag auf Zustimmung zu dem Fünften Gesetz zur Änderung des Öko-Landbaugesetzes"
    sheet_file = Path("some_other_name.xlsx")
    title, date = handle_title_and_date(full_title, sheet_file)
    assert (
        title
        == "Entschließungsantrag zu dem Antrag auf Zustimmung zu dem Fünften Gesetz zur Änderung des Öko-Landbaugesetzes"
    )
    assert date == pd.Timestamp("2020-11-26")


def test_handle_title_and_date_with_multiple_colons():
    """
    Tests that only the first part is treated as a date, even with multiple colons.
    """
    full_title = "26.11.2020: Title: with colons"
    sheet_file = Path("some_other_name.xlsx")
    title, date = handle_title_and_date(full_title, sheet_file)
    assert title == "Title: with colons"
    assert date == pd.Timestamp("2020-11-26")


def test_handle_title_and_date_with_date_in_filename():
    """
    Tests extraction when the date is not in the title but in the filename.
    """
    full_title = "Entschließungsantrag zu dem Antrag auf Zustimmung zu dem Fünften Gesetz zur Änderung des Öko-Landbaugesetzes"
    sheet_file = Path("20201126_3_xls-data.xlsx")
    title, date = handle_title_and_date(full_title, sheet_file)
    assert (
        title
        == "Entschließungsantrag zu dem Antrag auf Zustimmung zu dem Fünften Gesetz zur Änderung des Öko-Landbaugesetzes"
    )
    assert date == pd.Timestamp("2020-11-26")


def test_handle_title_and_date_no_date():
    """
    Tests behavior when no date is found in the title or filename.
    """
    full_title = "A title without a date"
    sheet_file = Path("a_file_without_a_date.xlsx")
    title, date = handle_title_and_date(full_title, sheet_file)
    assert title == "A title without a date"
    assert date is None


def test_handle_title_and_date_strips_whitespace():
    """
    Tests that leading/trailing whitespace is stripped from the title.
    """
    full_title = "  A title with whitespace   "
    sheet_file = Path("a_file.xlsx")
    title, date = handle_title_and_date(full_title, sheet_file)
    assert title == "A title with whitespace"
    assert date is None

    full_title_with_date = " 01.01.2021: A title with a date and whitespace  "
    title, date = handle_title_and_date(full_title_with_date, sheet_file)
    assert title == "A title with a date and whitespace"
    assert date == pd.Timestamp("2021-01-01")


# ========================= disambiguate_party =========================


def test_disambiguate_party():
    col = "Fraktion/Gruppe"
    known = list(PARTY_MAP.keys())
    unknown = ["wuppety"]
    df = pl.DataFrame({col: known + unknown})

    # line to test
    df2 = disambiguate_party(df, col=col, party_map=PARTY_MAP)

    assert df2[col].last() == df[col].last()
    assert not df2[col].equals(df[col])


# ========================= get_sheet_df =========================


@pytest.fixture
def create_dummy_excel(tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    def wrapped(fname: str, data: dict) -> Path:
        path = data_dir / fname
        file_size_zero = len(data) == 0
        if file_size_zero:
            path.touch()
            return path
        df = pl.DataFrame(data)
        df.write_excel(path)
        return path

    return wrapped


def test_get_sheet_df_valid_file(create_dummy_excel: Callable):
    data = {
        "Wahlperiode": [1, 1, 1],
        "Sitzungnr": [1, 1, 1],
        "Abstimmnr": [1, 2, 3],
        "Fraktion/Gruppe": ["CDU/CSU", "SPD", "BÜNDNIS`90/DIE GRÜNEN"],
        "Name": ["a", "b", "c"],
        "Vorname": ["d", "e", "f"],
        "Titel": ["von", "", None],
        "ja": [1, 0, 0],
        "nein": [0, 1, 0],
        "Enthaltung": [0, 0, 1],
        "ungültig": [0, 0, 0],
        "nichtabgegeben": [0, 0, 0],
        "Bezeichnung": ["a", "b", "c"],
        "Bemerkung": ["a", "aa", "aaa"],
    }
    sheet_file = create_dummy_excel("test.xlsx", data)

    df = get_sheet_df(sheet_file)
    assert df is not None
    assert len(df) == 3
    assert "BÜ90/GR" in df["Fraktion/Gruppe"].to_list()


def test_get_sheet_df_invalid_vote_columns(create_dummy_excel: Callable):
    data = {
        "Wahlperiode": [
            1,
        ],
        "Sitzungnr": [
            1,
        ],
        "Abstimmnr": [
            1,
        ],
        "Fraktion/Gruppe": ["CDU/CSU"],
        "Name": [
            "a",
        ],
        "Vorname": [
            "d",
        ],
        "Titel": [
            "von",
        ],
        "ja": [1],
        "nein": [1],
        "Enthaltung": [0],
        "ungültig": [0],
        "nichtabgegeben": [0],
        "Bezeichnung": ["a"],
        "Bemerkung": [
            "a",
        ],
    }
    sheet_file = create_dummy_excel("empty.xlsx", data)

    with pytest.raises(ValueError, match="has rows with more than one vote"):
        get_sheet_df(sheet_file)


def test_get_sheet_df_with_file_title_maps(create_dummy_excel: Callable):
    data = {
        "Wahlperiode": [
            1,
        ],
        "Sitzungnr": [
            1,
        ],
        "Abstimmnr": [
            1,
        ],
        "Fraktion/Gruppe": ["CDU/CSU"],
        "Name": [
            "a",
        ],
        "Vorname": [
            "d",
        ],
        "Titel": [
            "von",
        ],
        "ja": [1],
        "nein": [0],
        "Enthaltung": [0],
        "ungültig": [0],
        "nichtabgegeben": [0],
        "Bezeichnung": ["a"],
        "Bemerkung": [
            "a",
        ],
    }

    sheet_file = create_dummy_excel("20230101_1_test.xlsx", data)

    file_title_maps = {sheet_file.name: "01.01.2023: Test Title"}

    df = get_sheet_df(sheet_file, file_title_maps=file_title_maps)
    assert df is not None
    assert "title" in df.columns
    assert "date" in df.columns
    assert df["title"].first() == "Test Title"
    assert df["date"].first() == datetime.datetime(2023, 1, 1)


def test_get_sheet_df_disambiguate_party(create_dummy_excel: Callable):
    data = {
        "Wahlperiode": [1, 1, 1],
        "Sitzungnr": [1, 1, 1],
        "Abstimmnr": [1, 2, 3],
        "Fraktion/Gruppe": ["BÜNDNIS`90/DIE GRÜNEN", "DIE LINKE", "fraktionslos"],
        "Name": ["a", "b", "c"],
        "Vorname": ["d", "e", "f"],
        "Titel": ["von", "", None],
        "ja": [1, 0, 0],
        "nein": [0, 1, 0],
        "Enthaltung": [0, 0, 1],
        "ungültig": [0, 0, 0],
        "nichtabgegeben": [0, 0, 0],
        "Bezeichnung": ["a", "b", "c"],
        "Bemerkung": ["a", "aa", "aaa"],
    }
    sheet_file = create_dummy_excel("party_test.xlsx", data)

    df = get_sheet_df(sheet_file)
    assert df is not None
    parties = df["Fraktion/Gruppe"].to_list()
    assert "BÜ90/GR" in parties
    assert "DIE LINKE." in parties
    assert "Fraktionslos" in parties


def test_get_sheet_df_with_actual_data():
    path = Path("tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    # line to test
    _ = get_sheet_df(path, file_title_maps=file_title_maps)


def test_get_squished_dataframe():
    path = Path("tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    df = get_sheet_df(path, file_title_maps=file_title_maps)

    if df is None:
        raise ValueError(f"df cannot be None here.")

    df = get_squished_dataframe(df)
    records_expected = [
        {
            "Wahlperiode": 19,
            "Sitzungnr": 195,
            "Abstimmnr": 3,
            "Fraktion/Gruppe": "CDU/CSU",
            "Name": "Abercron",
            "Vorname": "Michael",
            "Titel": "von",
            "Bezeichnung": "Dr. Michael von Abercron",
            "Bemerkung": None,
            "sheet_name": "",
            "date": "2020-11-26T00:00:00.000",
            "title": "\u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "issue": "2020-11-26 \u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "vote": "ja",
        },
        {
            "Wahlperiode": 19,
            "Sitzungnr": 195,
            "Abstimmnr": 3,
            "Fraktion/Gruppe": "CDU/CSU",
            "Name": "Albani",
            "Vorname": "Stephan",
            "Titel": None,
            "Bezeichnung": "Stephan Albani",
            "Bemerkung": None,
            "sheet_name": "",
            "date": "2020-11-26T00:00:00.000",
            "title": "\u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "issue": "2020-11-26 \u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "vote": "ja",
        },
        {
            "Wahlperiode": 19,
            "Sitzungnr": 195,
            "Abstimmnr": 3,
            "Fraktion/Gruppe": "CDU/CSU",
            "Name": "Altenkamp",
            "Vorname": "Norbert Maria",
            "Titel": None,
            "Bezeichnung": "Norbert Maria Altenkamp",
            "Bemerkung": None,
            "sheet_name": "",
            "date": "2020-11-26T00:00:00.000",
            "title": "\u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "issue": "2020-11-26 \u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "vote": "ja",
        },
        {
            "Wahlperiode": 19,
            "Sitzungnr": 195,
            "Abstimmnr": 3,
            "Fraktion/Gruppe": "CDU/CSU",
            "Name": "Altmaier",
            "Vorname": "Peter",
            "Titel": None,
            "Bezeichnung": "Peter Altmaier",
            "Bemerkung": None,
            "sheet_name": "",
            "date": "2020-11-26T00:00:00.000",
            "title": "\u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "issue": "2020-11-26 \u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "vote": "nichtabgegeben",
        },
        {
            "Wahlperiode": 19,
            "Sitzungnr": 195,
            "Abstimmnr": 3,
            "Fraktion/Gruppe": "CDU/CSU",
            "Name": "Amthor",
            "Vorname": "Philipp",
            "Titel": None,
            "Bezeichnung": "Philipp Amthor",
            "Bemerkung": None,
            "sheet_name": "",
            "date": "2020-11-26T00:00:00.000",
            "title": "\u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "issue": "2020-11-26 \u00dcbereinkommen \u00fcber ein Einheitliches Patentgericht",
            "vote": "ja",
        },
    ]

    df_expected = pl.from_dicts(records_expected)
    df_expected = df_expected.with_columns(
        **{"date": pl.col("date").str.to_datetime().dt.date()}
    )

    df_head = df.head()

    for c in df_expected.columns:
        s_expected = df_expected[c]
        s = df_head[c]
        assert s_expected.equals(s)


@pytest.mark.parametrize("one_fails", [True, False])
def test_get_multiple_sheets(one_fails: bool):
    sheet_files = [
        Path("tests/data_for_testing/raw/bundestag/sheets/20201126_3_xls-data.xlsx"),
        Path("tests/data_for_testing/raw/bundestag/sheets/20201126_2_xls-data.xlsx"),
    ]
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
    }

    _ = get_multiple_sheets_df(
        sheet_files, file_title_maps=file_title_maps, validate=True
    )


@pytest.mark.parametrize(
    "dry,validate",
    [
        (
            dry,
            validate,
        )
        for dry in [True, False]
        for validate in [True, False]
    ],
)
def test_run(
    dry: bool,
    validate: bool,
    tmp_path: Path,
):
    html_path = Path("tests/data_for_testing")
    sheet_path = Path("tests/data_for_testing/raw/bundestag/sheets")
    preprocessed_path = tmp_path / "preprocessed"
    assume_yes = True

    run(
        html_dir=html_path,
        sheet_dir=sheet_path,
        preprocessed_path=preprocessed_path,
        dry=dry,
        validate=validate,
        assume_yes=assume_yes,
    )

    if dry:
        assert not preprocessed_path.exists()
    else:
        assert preprocessed_path.exists()
        output_file = preprocessed_path / "bundestag.de_votes.parquet"
        assert output_file.exists()


# ========================= create_vote_column =========================


def test_create_vote_column():
    """Tests that the vote column is created correctly based on the individual vote columns."""
    data = {
        "ja": [1, 0, 0, 0, 0, 0],
        "nein": [0, 1, 0, 0, 0, 0],
        "Enthaltung": [0, 0, 1, 0, 0, 0],
        "ungültig": [0, 0, 0, 1, 0, 0],
        "nichtabgegeben": [0, 0, 0, 0, 1, 0],
    }
    df = pl.DataFrame(data)

    # line to test
    df_with_vote = create_vote_column(df)

    expected_votes = ["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben", "error"]

    assert "vote" in df_with_vote.columns
    assert df_with_vote["vote"].to_list() == expected_votes


def test_create_vote_column_all_zero():
    """Tests the case where all vote columns are zero, expecting 'error'."""
    data = {
        "ja": [0],
        "nein": [0],
        "Enthaltung": [0],
        "ungültig": [0],
        "nichtabgegeben": [0],
    }
    df = pl.DataFrame(data)

    df_with_vote = create_vote_column(df)

    assert df_with_vote["vote"].to_list() == ["error"]
