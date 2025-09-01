import typing as T

import polars as pl
import pytest

from bundestag.data.transform.abgeordnetenwatch.helper import (
    extract_party_from_string,
    get_parties_from_col,
)


@pytest.mark.parametrize(
    "s,expected",
    [
        ("DIE LINKE seit 19.08.2021", "DIE LINKE"),
        ("AfD seit 20.07.2021", "AfD"),
        (42, None),
        ("blaaaaa", "blaaaaa"),
    ],
)
def test_extract_party_from_string(s: str, expected: str):
    try:
        # line to test
        res = extract_party_from_string(s)
    except ValueError as ex:
        if s == 42:
            pytest.xfail("ValueError for non-string input")
        else:
            raise ex
    assert res == expected


@pytest.mark.parametrize(
    "entries,targets",
    [
        (["DIE LINKE seit 19.08.2021"], ["DIE LINKE"]),
        (
            ["AfD seit 20.07.2021", "CDU/CSU seit 01.07.2021"],
            ["AfD", "CDU/CSU"],
        ),
        ([], []),
        (None, ["unknown"]),
    ],
)
def test_get_parties_from_col(entries: T.List[str], targets: T.List[str]):
    elements = pl.Series(entries)
    # line to test
    res = get_parties_from_col(elements, missing="unknown")
    assert all([targ == res[i] for i, targ in enumerate(targets)])
