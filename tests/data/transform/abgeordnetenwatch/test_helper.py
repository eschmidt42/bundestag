import typing as T

import pandas as pd
import pytest

from bundestag.data.transform.abgeordnetenwatch.helper import (
    extract_party_from_string,
    get_parties_from_col,
    get_politician_names,
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
    col = "fraction_names"
    row = pd.Series({col: entries})
    # line to test
    res = get_parties_from_col(row, col=col, missing="unknown")
    assert all([targ == res[i] for i, targ in enumerate(targets)])


def test_get_politician_names():
    col = "mandate"
    df = pd.DataFrame(
        {
            col: [
                "Zeki Gökhan (Bundestag 2017 - 2021)",
                "bla blaaaa blah (Bundestag 2017 - 2021)",
                "wup (Bundestag 2022 - 2025)",
            ]
        }
    )

    # line to test
    names = get_politician_names(df, col=col)

    assert names[0] == "Zeki Gökhan"
    assert names[1] == "bla blaaaa blah"
    assert names[2] == "wup"
