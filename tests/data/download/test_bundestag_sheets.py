import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from inline_snapshot import snapshot

from bundestag.data.download.bundestag_sheets import (
    RE_HTM,
    RE_SHEET,
    Source,
    collect_sheet_uris,
    create_xlsx_uris_dict,
    download_multiple_sheets,
    download_sheet,
    get_sheet_path,
    run,
    store_xlsx_uris,
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


def test_create_xlsx_uris_dict(monkeypatch):
    """Test create_xlsx_uris_dict by mocking selenium webdriver and page content.

    We simulate two pages. Each page's `page_source` contains an HTML snippet
    with one .xlsx link. The 'next' button is present on the first page and
    disabled on the second page to stop pagination.
    """

    from types import SimpleNamespace

    # Prepare two HTML snippets with a link ending in .xlsx
    html_page_1 = """
    <table>
      <tr><td data-th="Dokument"><div><p><strong>Title 1</strong></p><a href="https://example.com/file1.xlsx">xlsx</a></div></td></tr>
    </table>
    """

    html_page_2 = """
    <table>
      <tr><td data-th="Dokument"><div><p><strong>Title 2</strong></p><a href="https://example.com/file2.xlsx">xlsx</a></div></td></tr>
    </table>
    """

    pages = [html_page_1, html_page_2]
    fake_driver = MagicMock()
    fake_driver.page_source = pages[0]

    # state counter to track find_element calls
    state = {"find_calls": 0}

    def find_element(by, selector):
        # first call: return enabled button whose click advances the page
        if state["find_calls"] == 0:
            btn = MagicMock()

            def click_side_effect():
                fake_driver.page_source = pages[1]

            btn.click.side_effect = click_side_effect
            btn.get_attribute.side_effect = lambda name: "false"
            state["find_calls"] += 1
            return btn
        # subsequent calls: return disabled button
        btn = MagicMock()
        btn.get_attribute.side_effect = lambda name: "true"
        return btn

    fake_driver.find_element.side_effect = find_element
    fake_driver.get.return_value = None
    fake_driver.execute_script.return_value = None
    fake_driver.quit.return_value = None

    fake_webdriver = SimpleNamespace(Chrome=lambda: fake_driver)
    monkeypatch.setitem(__import__("sys").modules, "selenium.webdriver", fake_webdriver)

    # Also patch the By constant module used in the function
    monkeypatch.setitem(
        __import__("sys").modules,
        "selenium.webdriver.common.by",
        SimpleNamespace(By=SimpleNamespace(CSS_SELECTOR="button.slick-next")),
    )

    # Run the function
    uris = create_xlsx_uris_dict(max_pages=2, t_wait_scroll=0.0, t_wait_load=0.0)

    # Expect two entries with titles 'Title 1' and 'Title 2'
    assert any("Title 1" in k for k in uris.keys())
    assert any("Title 2" in k for k in uris.keys())


def test_store_xlsx_uris(tmp_path: Path):
    """Test that store_xlsx_uris writes the given dict to a JSON file correctly."""

    json_path = tmp_path / "xlsx_uris.json"
    sample = {
        "Title A": "https://example.com/a.xlsx",
        "Title B": "https://example.com/b.xlsx",
    }

    store_xlsx_uris(json_path, sample)

    # file should exist and contain the same data
    assert json_path.exists()
    with json_path.open("r") as f:
        data = json.load(f)

    assert data == sample


def test_run_creates_paths_and_json_and_downloads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """When html_dir and sheet_dir don't exist and do_create_xlsx_uris_json is True
    with source=Source.json_file, `run` should call `create_xlsx_uris_dict`, write the
    JSON to the parent of html_dir, ensure directories are created, and call
    `download_multiple_sheets` with the produced dict.
    """

    # prepare paths that don't exist yet
    html_dir = tmp_path / "html_dir" / "sub"
    sheet_dir = tmp_path / "sheets_dir"

    # sample dict that create_xlsx_uris_dict should return
    sample_uris = {"T1": "https://example.com/1.xlsx"}

    # monkeypatch create_xlsx_uris_dict to return our sample and avoid selenium
    # make patched function accept the same signature (with defaults) as the real one
    monkeypatch.setattr(
        "bundestag.data.download.bundestag_sheets.create_xlsx_uris_dict",
        lambda max_pages=1, t_wait_scroll=0.0, t_wait_load=0.0: sample_uris,
    )

    called = {}

    def fake_download_multiple_sheets(uris, sheet_dir, t_sleep, nmax, dry):
        called["uris"] = uris
        called["sheet_dir"] = Path(sheet_dir)

    monkeypatch.setattr(
        "bundestag.data.download.bundestag_sheets.download_multiple_sheets",
        fake_download_multiple_sheets,
    )

    # run with do_create_xlsx_uris_json True
    run(
        html_dir,
        sheet_dir,
        t_sleep=0.0,
        nmax=None,
        dry=False,
        pattern=RE_HTM,
        assume_yes=True,
        source=Source.json_file,
        json_filename="xlsx_uris.json",
        do_create_xlsx_uris_json=True,
        max_pages=1,
    )

    # JSON should have been written to html_dir.parent / json_filename
    json_path = html_dir.parent / "xlsx_uris.json"
    assert json_path.exists()
    with json_path.open("r") as f:
        import json

        data = json.load(f)
    assert data == sample_uris

    # directories should have been created
    assert html_dir.parent.exists()
    assert sheet_dir.exists()

    # download_multiple_sheets should have been called with our sample dict
    assert called.get("uris") == sample_uris
    assert called.get("sheet_dir") == sheet_dir
