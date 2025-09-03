import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from inline_snapshot import snapshot

from bundestag.data.download.bundestag_sheets import (
    RE_HTM,
    RE_SHEET,
    collect_sheet_uris,
    download_multiple_sheets,
    download_sheet,
    get_sheet_path,
    run,
)


@pytest.mark.parametrize(
    "p,href,pattern",
    [
        pytest.param(
            Path("tests/data_for_testing/test-1.html"),
            "https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx",
            RE_SHEET,
            id="xlsx+re_pattern",
        ),
        pytest.param(
            Path("tests/data_for_testing/test-2.html"),
            None,
            RE_SHEET,
            id="no_xlsx+re_pattern",
        ),
        pytest.param(
            Path("tests/data_for_testing/test-1.html"),
            "https://www.bundestag.de/resource/blob/810078/41aca52ecb2c567c3851ebd15e7d9475/20201126_3_xls-data.xlsx",
            None,
            id="xlsx+no_pattern",
        ),
        pytest.param(
            Path("tests/data_for_testing/test-2.html"),
            None,
            None,
            id="no_xlsx+no_pattern",
        ),
    ],
)
def test_collect_sheet_uris(p: Path, href: str, pattern: re.Pattern):
    title = "26.11.2020: Übereinkommen über ein Einheitliches Patentgericht"

    uris = collect_sheet_uris([p], pattern=pattern)

    if href is None:
        assert title not in uris
    else:
        assert uris[title] == href


@pytest.mark.parametrize(
    "uri,sheet_path,expected",
    [
        (
            "https://www.bundestag.de/resource/blob/810078/file.xlsx",
            "data/sheets",
            Path("data/sheets/file.xlsx"),
        ),
        (
            "https://example.com/path/to/document.xls",
            Path("output/dir"),
            Path("output/dir/document.xls"),
        ),
        ("file.xlsx", "sheets", Path("sheets/file.xlsx")),
        ("nested/path/file.xlsx", Path("base"), Path("base/file.xlsx")),
    ],
    ids=["full_url_str_path", "full_url_path_object", "simple_filename", "nested_path"],
)
def test_get_sheet_path(uri: str, sheet_path, expected: Path):
    result = get_sheet_path(uri, sheet_path)
    assert result == expected
    assert isinstance(result, Path)


@pytest.mark.parametrize("dry", [True, False])
def test_download_sheet(dry: bool, tmp_path: Path):
    uri = "www.somewhere.org/file.csv"
    sheet_dir = tmp_path / "bundestag_sheets"
    sheet_path = get_sheet_path(uri, sheet_dir)

    r = httpx.Response(200, json={"wuppety": 42})

    with (
        patch("httpx.get", MagicMock(return_value=r)) as _get,
    ):
        # line to test
        download_sheet(uri, sheet_dir, dry=dry)

        if dry:
            assert _get.call_count == 0
            assert not sheet_path.exists()
        else:
            assert _get.call_count == 1
            assert sheet_path.exists()
            with sheet_path.open("rb") as f:
                assert f.read() == snapshot(b'{"wuppety":42}')


@pytest.mark.parametrize("nmax", [None, 2])
def test_download_multiple_sheets(nmax: int, tmp_path: Path):
    uris = {
        "dummy-name#1": "www.someplace.org/sheet1.xlsx",
        "dummy-name#2": "www.someplace.org/sheet2.xlsx",
        "dummy-name#3": "www.someplace.org/sheet3.xlsx",
        "dummy-name#4": "www.someplace.org/sheet4.xlsx",
    }
    sheet_dir = tmp_path / "bundestag_sheets"
    t_sleep = 0.0
    dry = False
    known_sheets = [sheet_dir / "sheet1.xlsx"]

    sheet_dir.mkdir()
    for p in known_sheets:
        with p.open("w") as f:
            f.write("wuppety")

    r = httpx.Response(200, json={"wuppety": 42})

    with (
        patch("httpx.get", MagicMock(return_value=r)) as _get,
    ):
        # line to test
        download_multiple_sheets(uris, sheet_dir, t_sleep=t_sleep, nmax=nmax, dry=dry)

        assert sheet_dir.exists()
        n_files = len(list(sheet_dir.iterdir()))
        if nmax is None:
            assert _get.call_count == 3
            assert n_files == 4
        else:
            assert _get.call_count == 1
            assert n_files == 2


def test_run(tmp_path: Path):
    html_dir = Path("tests/data_for_testing")
    sheet_dir = tmp_path / "sheets"

    sheet_dir.mkdir()

    t_sleep = 0.0
    nmax = None
    dry = False
    pattern = RE_HTM
    assume_yes = False

    r = httpx.Response(200, json={"wuppety": 42})

    with (
        patch("httpx.get", MagicMock(return_value=r)) as _get,
    ):
        run(
            html_dir,
            sheet_dir,
            t_sleep=t_sleep,
            nmax=nmax,
            dry=dry,
            pattern=pattern,
            assume_yes=assume_yes,
        )

        assert sheet_dir.exists()
        n_files = len(list(sheet_dir.iterdir()))
        if nmax is None:
            assert _get.call_count == snapshot(1)
            assert n_files == snapshot(1)
