import re
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

import bundestag.data.download.bundestag_sheets as hp
import bundestag.schemas as schemas


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
            "bundestag.data.utils.get_file_paths",
            MagicMock(return_value=known_sheets),
        ) as get_file_paths,
        patch(
            "bundestag.data.download.bundestag_sheets.download_sheet",
            MagicMock(),
        ) as download_sheet,
    ):
        # line to test
        hp.download_multiple_sheets(
            uris, sheet_path, t_sleep=t_sleep, nmax=nmax, dry=dry
        )

        get_file_paths.assert_called_once()
        download_sheet.assert_called_once()
