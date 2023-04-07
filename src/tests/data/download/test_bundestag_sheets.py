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


@pytest.mark.parametrize(
    "s,href,pattern",
    [
        (s, href, pattern)
        for pattern in [hp.RE_SHEET, None]
        for (s, href) in [
            (
                """
    <td data-th="Dokument">
      <div class="bt-documents-description">
        <p><strong>
        26.11.2020: Übereinkommen über ein Einheitliches Patentgericht</strong></p>
        <ul class="bt-linkliste">
              <li><a title="PDF | 176 KB" class="bt-link-dokument" href="https://www.bundestag.de/resource/blob/810080/6aec44794420f1c45739567b682af84d/20201126_3-data.pdf" target="_blank" tabindex="0">  PDF | 176 KB</a></li>
              <li><a title="XLSX | 51 KB" class="bt-link-dokument" href="https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx" target="_blank" tabindex="0">  XLSX | 51 KB</a></li>
        </ul>
      </div>
    </td>""",
                "https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx",
            ),
            (
                """
    <td data-th="Dokument">
      <div class="bt-documents-description">
        <p><strong>
        26.11.2020: Übereinkommen über ein Einheitliches Patentgericht</strong></p>
        <ul class="bt-linkliste">
              <li><a title="PDF | 176 KB" class="bt-link-dokument" href="https://www.bundestag.de/resource/blob/810080/6aec44794420f1c45739567b682af84d/20201126_3-data.pdf" target="_blank" tabindex="0">  PDF | 176 KB</a></li>
        </ul>
      </div>
    </td>""",
                None,
            ),
        ]
    ],
    ids=[
        "xlsx+re_pattern",
        "no_xlsx+re_pattern",
        "xlsx+no_pattern",
        "no_xlsx+no_pattern",
    ],
)
def test_collect_sheet_uris(s: str, href: str, pattern: re.Pattern):
    title = "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht"

    html_file_paths = [Path("dummy/path/f1.html")]
    with patch("builtins.open", mock_open(read_data=s)):
        # line to test
        uris = hp.collect_sheet_uris(html_file_paths, pattern=pattern)

    if href is None:
        assert title not in uris
    else:
        assert uris[title] == href


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


@pytest.mark.parametrize("nmax", [None, 2])
def test_download_multiple_sheets(nmax: int):
    uris = {
        "dummy-name#1": "www.someplace.org/sheet1.xlsx",
        "dummy-name#2": "www.someplace.org/sheet2.xlsx",
        "dummy-name#3": "www.someplace.org/sheet3.xlsx",
        "dummy-name#4": "www.someplace.org/sheet4.xlsx",
    }
    sheet_path = Path("dummy/place")
    t_sleep = 0.0
    # nmax = None
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

        assert get_file_paths.call_count == 1
        if nmax is None:
            assert download_sheet.call_count == 3
        else:
            assert download_sheet.call_count == 2


@pytest.mark.parametrize("path_exists", [True, False])
def test_run(path_exists: bool):
    html_path = Path("dummy/path/html")
    sheet_path = Path("dummy/path/sheet")
    t_sleep = 0.0
    nmax = None
    dry = False
    pattern = hp.RE_HTM

    with (
        patch(
            "pathlib.Path.exists", MagicMock(return_value=path_exists)
        ) as _path_exists,
        patch(
            "bundestag.data.utils.ensure_path_exists", MagicMock()
        ) as _ensure_path_exists,
        patch(
            "bundestag.data.utils.get_file_paths", MagicMock(return_value=[])
        ) as _get_file_paths,
        patch(
            "bundestag.data.download.bundestag_sheets.collect_sheet_uris",
            MagicMock(return_value=[]),
        ) as _collect_sheet_uris,
        patch(
            "bundestag.data.download.bundestag_sheets.download_multiple_sheets",
            MagicMock(),
        ) as _download_multiple_sheets,
    ):
        # line to test
        hp.run(
            html_path,
            sheet_path,
            t_sleep=t_sleep,
            nmax=nmax,
            dry=dry,
            pattern=pattern,
        )

        assert _path_exists.call_count == 2

        assert _ensure_path_exists.call_count == 2 * int(not path_exists)
        _get_file_paths.assert_called_once_with(html_path, pattern=pattern)
        _collect_sheet_uris.assert_called_once_with([])
        _download_multiple_sheets.assert_called_once_with(
            [], sheet_path=sheet_path, t_sleep=t_sleep, nmax=nmax, dry=dry
        )
