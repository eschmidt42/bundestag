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

        assert json_load.call_count == (0 if dry else 1)
        assert _open.call_count == (0 if dry else 1)


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


@pytest.mark.parametrize(
    "user_inputs,expected,max_tries",
    [
        (["y"], True, 1),
        (["Y"], True, 1),
        ([""], True, 1),
        ([None], True, 1),
        (["n"], False, 1),
        (["N"], False, 1),
        (["no", "nein", "n"], False, 3),
        (["no", "nein", "n"], "fail", 2),
        (["yes"], "fail", 1),
        (["yes", "y"], True, 2),
        ([1], "fail", 1),
    ],
)
def test_get_user_path_creation_decision(
    user_inputs, expected, max_tries: int
):
    with (patch("builtins.input", side_effect=user_inputs) as _input,):
        try:
            # line to test
            decision = data_utils.get_user_path_creation_decision(
                Path("dummy/path"), max_tries=max_tries
            )
        except ValueError as ex:
            if expected == "fail":
                pytest.xfail(
                    "Expected fail due to wrong user inputs exceeding the max_tries"
                )
            else:
                raise ex

        assert decision == expected


@pytest.mark.parametrize("do_creation", [True, False])
def test_ensure_path_exists(do_creation: bool):
    path = Path("dummy/path")
    with (
        patch(
            "bundestag.data.utils.get_user_path_creation_decision",
            return_value=do_creation,
        ) as _do_creation,
        patch("pathlib.Path.mkdir") as _mkdir,
    ):
        # line to test
        data_utils.ensure_path_exists(path)

        assert _do_creation.call_count == 1
        assert _do_creation.call_args[0][0] == path
        assert _mkdir.call_count == (1 if do_creation else 0)
