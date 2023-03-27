import re
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

import bundestag.html_parsing as hp
import bundestag.schemas as schemas

PATTERN = re.compile("\.txt")


@pytest.mark.parametrize(
    "suffix,pattern",
    [
        ("*.txt", None),
        (None, PATTERN),
        (None, None),
        ("*.txt", PATTERN),
    ],
)
def test_get_file_paths(suffix: str, pattern: re.Pattern):
    path = Path("dummy/path")
    files = [path / "f1.txt", path / "f2.txt", path / "f3.txxt"]
    file0, file1, file2 = files

    with (
        patch("pathlib.Path.rglob", return_value=[file0, file1]) as rglob,
        patch("pathlib.Path.glob", return_value=files) as glob,
    ):
        # line to test
        try:
            found_files = hp.get_file_paths(
                path, suffix=suffix, pattern=pattern
            )
        except NotImplementedError as ex:
            if suffix is None and pattern is None:
                pytest.xfail("Expected fail due to missing pattern and suffix")
            else:
                raise ex

    assert set(found_files) == set([file0, file1])


def test_collect_sheet_uris():
    s = """
    <td data-th="Dokument">
      <div class="bt-documents-description">
        <p><strong>
        26.11.2020: Übereinkommen über ein Einheitliches Patentgericht</strong></p>
        <ul class="bt-linkliste">
              <li><a title="PDF | 176 KB" class="bt-link-dokument" href="https://www.bundestag.de/resource/blob/810080/6aec44794420f1c45739567b682af84d/20201126_3-data.pdf" target="_blank" tabindex="0">  PDF | 176 KB</a></li>
              <li><a title="XLSX | 51 KB" class="bt-link-dokument" href="https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx" target="_blank" tabindex="0">  XLSX | 51 KB</a></li>
                  </ul>
          </div>
    </td>
    """
    uri = "https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx"
    title = "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht"

    html_file_paths = [Path("dummy/path/f1.html")]
    with patch("builtins.open", mock_open(read_data=s)):
        res = hp.collect_sheet_uris(html_file_paths, pattern=hp.RE_SHEET)

    assert title in res
    assert res[title] == uri


@pytest.mark.parametrize("dry", [True, False])
def test_download_sheet(dry: bool):
    uri = "www.somewhere.org/file.csv"
    sheet_path = Path("some/place")
    r = requests.Response()
    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("requests.get", MagicMock(return_value=r)) as _get,
    ):
        # line to test
        hp.download_sheet(uri, sheet_path, dry=dry)

        _mkdir.assert_called_once()
        if dry:
            assert _open.call_count == 0
            assert _get.call_count == 0
        else:
            assert _open.call_count == 1
            assert _get.call_count == 1
            assert _open.return_value.__enter__().write.call_count == 1


def test_get_sheet_fname():
    s = "wup/petty"

    # line to test
    r = hp.get_sheet_fname(s)

    assert r == "petty"


def test_download_multiple_sheets():
    uris = {
        "dummy-name#1": "www.someplace.org/sheet1.xlsx",
        "dummy-name#2": "www.someplace.org/sheet2.xlsx",
    }
    sheet_path = Path("dummy/place")
    t_sleep = 0.0
    nmax = None
    dry = False
    known_sheets = [sheet_path / "sheet1.xlsx"]

    with (
        patch(
            "bundestag.html_parsing.get_file_paths",
            MagicMock(return_value=known_sheets),
        ) as get_file_paths,
        patch(
            "bundestag.html_parsing.download_sheet", MagicMock()
        ) as download_sheet,
    ):
        # line to test
        hp.download_multiple_sheets(
            uris, sheet_path, t_sleep=t_sleep, nmax=nmax, dry=dry
        )

        get_file_paths.assert_called_once()
        download_sheet.assert_called_once()


def test_get_file2poll_maps():
    uris = {
        "title1": "uri/1.xlsx",
        "title2": "uri/2.xlsx",
    }
    sheet_path = Path("dummy/path")
    known_sheets = [sheet_path / "1.xlsx"]

    with patch(
        "bundestag.html_parsing.get_file_paths",
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


@pytest.mark.skip("Currently not functioning because no data")
class TestHTMLParsing(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.read_path = Path("./bundestag.de_data")
        self.write_path = Path("./bundestag.de_data_test")
        self.write_path.mkdir(exist_ok=True)
        self.html = "htm_files"
        self.sheet = "sheets"
        self.write_path.mkdir(exist_ok=True)
        (self.write_path / self.sheet).mkdir(exist_ok=True)
        self.dry = False
        self.nmax = 3

        self.html_file_paths = hp.get_file_paths(
            self.read_path / self.html, pattern=hp.RE_HTM
        )
        self.sheet_uris = hp.collect_sheet_uris(self.html_file_paths)

        hp.download_multiple_sheets(
            self.sheet_uris,
            sheet_path=self.write_path / self.sheet,
            nmax=self.nmax,
            dry=self.dry,
        )
        self.file_title_maps = hp.get_file2poll_maps(
            self.sheet_uris, self.write_path / self.sheet
        )

        self.sheet_files = hp.get_file_paths(
            self.write_path / self.sheet, pattern=hp.RE_FNAME
        )

        self.df = (
            hp.get_sheet_df(
                self.sheet_files[0], file_title_maps=self.file_title_maps
            )
            if len(self.sheet_files) > 0
            else None
        )

    # def test_0_file_paths(self):
    #     hp.test_file_paths(self.html_file_paths, self.read_path / self.html)

    # def test_1_sheet_uris(self):
    #     hp.test_sheet_uris(self.sheet_uris)

    # def test_2_file_title_maps(self):
    #     hp.test_file_title_maps(self.file_title_maps, self.sheet_uris)

    # def test_3_get_sheet_df(self):
    #     if self.df is not None:
    #         hp.test_get_sheet_df(self.df)

    # def test_4_squished_df(self):
    #     if self.df is not None:
    #         tmp = hp.get_squished_dataframe(self.df)
    #         hp.test_squished_df(tmp, self.df)
