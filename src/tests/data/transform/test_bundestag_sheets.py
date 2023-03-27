import re
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

import bundestag.data.transform.bundestag_sheets as hp
import bundestag.schemas as schemas


def test_get_file2poll_maps():
    uris = {
        "title1": "uri/1.xlsx",
        "title2": "uri/2.xlsx",
    }
    sheet_path = Path("dummy/path")
    known_sheets = [sheet_path / "1.xlsx"]

    with patch(
        "bundestag.data.utils.get_file_paths",
        MagicMock(return_value=known_sheets),
    ) as get_file_paths:
        # line to test
        file2poll = hp.get_file2poll_maps(uris, sheet_path)

        assert len(file2poll) == 1
        assert "1.xlsx" in file2poll
        assert file2poll["1.xlsx"] == "title1"


def test_is_date():
    assert hp.is_date("123", pd.to_datetime) == False
    assert hp.is_date("2022-02-02", pd.to_datetime) == True


def test_get_sheet_df():
    path = Path("src/tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    # line to test
    df = hp.get_sheet_df(path, file_title_maps=file_title_maps, validate=True)


@pytest.mark.parametrize(
    "full_title,sheet_file,exp_title,exp_date",
    [
        (
            "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            pd.to_datetime("2020-11-26"),
        ),
        (
            "Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/2020-11-26_bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            pd.to_datetime("2020-11-26"),
        ),
        (
            "Übereinkommen über ein Einheitliches Patentgericht",
            Path("some/file/bla.csv"),
            "Übereinkommen über ein Einheitliches Patentgericht",
            None,
        ),
    ],
)
def test_handle_title_and_date(
    full_title: str,
    sheet_file: Path,
    exp_title: str,
    exp_date: pd.DatetimeTZDtype,
):
    # full_title = "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht"
    # sheet_file = Path("some/file/2020-11-26_bla.csv")

    # line to test
    title, date = hp.handle_title_and_date(full_title, sheet_file)
    assert title == exp_title
    if exp_date is None:
        assert date is None
    else:
        assert date == exp_date


def test_disambiguate_party():
    col = "Fraktion/Gruppe"
    known = list(hp.PARTY_MAP.keys())
    unknown = ["wuppety"]
    df = pd.DataFrame({col: known + unknown})

    # line to test
    df2 = hp.disambiguate_party(df.copy(), col=col, party_map=hp.PARTY_MAP)

    assert df2[col].iloc[-1] == df[col].iloc[-1]
    assert not df2[col].iloc[:-1].equals(df[col].iloc[:-1])


# def test_squished_df(df_squished: pd.DataFrame, df: pd.DataFrame):
#     assert len(df_squished) == len(df)
#     assert "vote" in df_squished.columns
#     assert "issue" in df_squished.columns
#     assert not any([v in df_squished.columns for v in hp.VOTE_COLS])

# def test_get_squished_dataframe():
#     pass


@pytest.mark.skip("to be implemented")
def test_set_sheet_dtypes():
    pass


def test_get_final_sheet_df():
    path = Path("src/tests/data_for_testing/20201126_3_xls-data.xlsx")
    file_title_maps = {
        "20201126_3_xls-data.xlsx": "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht",
        "20201126_2_xls-data.xlsx": "26.11.2020: Europäische Bank für nachhaltige Entwicklung (Beschlussempfehlung)",
        "20201118_4_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Antrag CDU/CSU, SPD",
        "20201118_1_xls-data.xlsx": "18.11.2020: Corona-Maßnahmen (epidemische Lage), Änderungsantrag FDP",
        "20200916_1_xls-data.xlsx": "16.09.2020: Mobilität der Zukunft (Beschlussempfehlung)",
    }

    df = hp.get_sheet_df(path, file_title_maps=file_title_maps, validate=False)

    # lines to test
    df = hp.get_squished_dataframe(df)
    df = hp.set_sheet_dtypes(df)
    schemas.SHEET_FINAL.validate(df)


@pytest.mark.skip("to be implemented")
def test_get_multiple_sheets():
    pass


# def test_file_paths(html_file_paths: list, html_path: Path):
#     # logger.debug("Sanity checking the number of found files")
#     assert len(html_file_paths) > 0
#     assert len(html_file_paths) >= len(
#         hp.get_file_paths(html_path, suffix=".htm")
#     )


# def test_sheet_uris(sheet_uris):
#     # logger.debug("Sanity checking collected URIs to excel sheets")
#     assert isinstance(sheet_uris, dict)
#     assert (
#         "10.09.2020: Abstrakte Normenkontrolle - Düngeverordnung (Beschlussempfehlung)"
#         in sheet_uris
#     )


# def test_file_title_maps(
#     file_poll_title_maps: T.Dict[str, str], uris: T.Dict[str, str]
# ):
#     assert len(file_poll_title_maps) <= len(uris)
