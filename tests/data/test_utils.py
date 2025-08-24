import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest

from bundestag.data.utils import (
    ensure_path_exists,
    get_file_paths,
    get_location,
    get_mandates_filename,
    get_polls_filename,
    get_sheet_filename,
    get_user_path_creation_decision,
    get_votes_filename,
    load_json,
)


def test_get_location():
    fname = "wup.csv"
    path = Path("file/location")

    file = get_location(fname=fname, path=path, dry=True, mkdir=False)
    assert file == path / fname


def test_polls_filename():
    assert get_polls_filename(42) == "polls_legislature_42.json"


def test_mandates_filename():
    assert get_mandates_filename(42) == "mandates_legislature_42.json"


def test_votes_filename():
    assert get_votes_filename(42, 21) == "votes_legislature_42/poll_21_votes.json"


def test_get_sheet_filename():
    s = "wup/petty"

    # line to test
    r = get_sheet_filename(s)

    assert r == "petty"


@pytest.mark.parametrize("dry", [True, False])
def test_load_json(dry: bool, tmp_path: Path):
    path = tmp_path / "name.json"

    expected = {"wupptey": 42}
    with path.open("w") as f:
        json.dump(expected, f)

    info = load_json(path, dry=dry)

    match dry:
        case True:
            assert info == {}
        case False:
            assert info == expected


PATTERN = re.compile(r"\.txt")


@pytest.mark.parametrize(
    "suffix, pattern_str, files_to_create, expected_files, should_fail",
    [
        # Test case 1: Suffix search
        (
            "*.txt",
            None,
            ["f1.txt", "f2.txt", "sub/f3.txt", "f4.log"],
            ["f1.txt", "f2.txt", "sub/f3.txt"],
            False,
        ),
        # Test case 2: Pattern search
        (
            None,
            r"\.txt$",
            ["f1.txt", "f2.txt", "f3.txxt", "sub/f4.txt"],
            ["f1.txt", "f2.txt", "sub/f4.txt"],
            False,
        ),
        # Test case 3: No suffix or pattern
        (None, None, [], [], True),
        # Test case 4: Suffix takes precedence over pattern
        (
            "*.txt",
            r"\.log$",
            ["f1.txt", "f2.log", "sub/f3.txt"],
            ["f1.txt", "sub/f3.txt"],
            False,
        ),
        # Test case 5: Empty result
        ("*.md", None, ["f1.txt", "f2.log"], [], False),
    ],
)
def test_get_file_paths(
    suffix: str | None,
    pattern_str: str | None,
    files_to_create: list[str],
    expected_files: list[str],
    should_fail: bool,
    tmp_path: Path,
):
    """This test does not use any mocking."""
    for f_str in files_to_create:
        f_path = tmp_path / f_str
        f_path.parent.mkdir(parents=True, exist_ok=True)
        f_path.touch()

    pattern = re.compile(pattern_str) if pattern_str else None

    if should_fail:
        with pytest.raises(NotImplementedError):
            get_file_paths(tmp_path, pattern=pattern, suffix=suffix)
    else:
        found_files = get_file_paths(tmp_path, pattern=pattern, suffix=suffix)
        expected_paths = {tmp_path / f for f in expected_files}
        assert set(found_files) == expected_paths


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
def test_get_user_path_creation_decision(user_inputs, expected, max_tries: int):
    with patch("builtins.input", side_effect=user_inputs):
        try:
            # line to test
            decision = get_user_path_creation_decision(
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


@pytest.mark.parametrize(
    "user_decision,assume_yes",
    [
        (user_decision, assume_yes)
        for user_decision in [True, False]
        for assume_yes in [True, False]
    ],
)
def test_ensure_path_exists(user_decision: bool, assume_yes: bool, tmp_path: Path):
    path = tmp_path / "created-dir"
    input_value = "y" if user_decision else "n"
    with patch("builtins.input", side_effect=(input_value,)):
        # line to test
        ensure_path_exists(path, assume_yes=assume_yes)

        match (assume_yes, user_decision):
            case (False, True):
                assert path.exists()
            case (False, False):
                assert not path.exists()
            case (True, True):
                assert path.exists()
            case (True, False):
                assert path.exists()
