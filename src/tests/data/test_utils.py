import re
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

import bundestag.data.utils as data_utils


def test_get_location():
    fname = "wup.csv"
    path = Path("file/location")

    file = data_utils.get_location(
        fname=fname, path=path, dry=True, mkdir=False
    )
    assert file == path / fname


def test_polls_file():
    assert data_utils.polls_file(42) == "polls_legislature_42.json"


def test_mandates_file():
    assert data_utils.mandates_file(42) == "mandates_legislature_42.json"


def test_votes_file():
    assert (
        data_utils.votes_file(42, 21)
        == "votes_legislature_42/poll_21_votes.json"
    )


def test_get_sheet_fname():
    s = "wup/petty"

    # line to test
    r = data_utils.get_sheet_fname(s)

    assert r == "petty"


@pytest.mark.parametrize("dry", [True, False])
def test_load_json(dry: bool):
    path = Path("file/path/name.json")

    with (
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.load", MagicMock()) as json_load,
    ):
        # line to test
        data_utils.load_json(path, dry=dry)

        assert json_load.call_count == 1
        assert _open.call_count == 1


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
            found_files = data_utils.get_file_paths(
                path, suffix=suffix, pattern=pattern
            )
        except NotImplementedError as ex:
            if suffix is None and pattern is None:
                pytest.xfail("Expected fail due to missing pattern and suffix")
            else:
                raise ex

    assert set(found_files) == set([file0, file1])
