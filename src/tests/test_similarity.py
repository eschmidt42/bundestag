from unittest.mock import MagicMock, patch

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

import bundestag.data.transform.bundestag_sheets as transform_bs
import bundestag.similarity as sim


@pytest.fixture(scope="module")
def df():
    return pd.DataFrame(
        {
            "Fraktion/Gruppe": ["A", "A", "A", "B", "B", "B"],
            "vote": ["ja", "nein", "Enthaltung", "ja", "nein", "Enthaltung"],
            "date": ["2022-02-02"] * 6,
            "title": ["bla"] * 6,
            "Bezeichnung": ["A", "B", "C", "D", "E", "F"],
        }
    )


def test_votes_by_party(df: pd.DataFrame):
    # line to test
    votes = sim.get_votes_by_party(df)

    expected_columns = [
        "Fraktion/Gruppe",
        "date",
        "title",
        "fraction",
        "vote",
        "# votes",
    ]
    assert all([v in votes.columns for v in expected_columns])
    assert votes["# votes"].isna().sum() == 0


def test_pivot_party_votes_df(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)

    # line to test
    pivoted = sim.pivot_party_votes_df(votes)

    vote_columns = ["ja", "nein", "Enthaltung"]
    other_columns = ["Fraktion/Gruppe"]
    assert all([v in pivoted.columns for v in vote_columns + other_columns])
    assert pivoted[vote_columns].isna().sum().sum() == 0


@pytest.mark.parametrize("mdb", ["A", "wup"])
def test_prepare_votes_of_mdb(mdb: str, df: pd.DataFrame):
    try:
        # line to test
        mdb_votes = sim.prepare_votes_of_mdb(df, mdb)
    except ValueError as ex:
        if mdb == "wup":
            pytest.xfail("mdb not found")
        else:
            raise ex

    expected_columns = ["date", "title"] + transform_bs.VOTE_COLS
    assert all([v in mdb_votes.columns for v in expected_columns])
    assert mdb_votes[transform_bs.VOTE_COLS].isna().sum().sum() == 0


def test_align_mdb_with_parties(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)
    mdb_votes = sim.prepare_votes_of_mdb(df, "A")

    # line to test
    mdb_vs_parties = sim.align_mdb_with_parties(mdb_votes, pivoted)

    expected_columns = (
        ["date", "title"]
        + [f"{v}_mdb" for v in transform_bs.VOTE_COLS]
        + [f"{v}_party" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe"]
    )
    assert all([v in mdb_vs_parties.columns for v in expected_columns])


@pytest.mark.parametrize(
    "a,b,expected",
    [
        (np.array([1, 0]), np.array([0, 1]), 0),
        (np.array([1, 0]), np.array([1, 0]), 1),
        (np.array([1, 0]), np.array([1, 1]), np.sqrt(2) / 2),
    ],
)
def test_cosine_similarity(a, b, expected):
    # line to test
    res = sim.cosine_similarity(a, b)

    assert np.isclose(res, expected)


def test_compute_similarity(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)
    mdb_votes = sim.prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = sim.align_mdb_with_parties(mdb_votes, pivoted)

    # line to test
    res = sim.compute_similarity(
        mdb_vs_parties, lsuffix="mdb", rsuffix="party"
    )

    assert all([v in res.columns for v in mdb_vs_parties.columns])
    assert "similarity" in res.columns
    assert res["similarity"].between(0, 1).all()


def test_align_party_with_party(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)

    # line to test
    res = sim.align_party_with_party(pivoted, party_a="A", party_b="B")

    expected_columns = (
        ["date", "title"]
        + [f"{v}_a" for v in transform_bs.VOTE_COLS]
        + [f"{v}_b" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe_a", "Fraktion/Gruppe_b"]
    )
    assert all([v in res.columns for v in expected_columns])


def test_align_party_with_all_parties(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)

    # line to test
    res = sim.align_party_with_all_parties(pivoted, party_a="A")

    expected_columns = (
        ["date", "title"]
        + [f"{v}_a" for v in transform_bs.VOTE_COLS]
        + [f"{v}_b" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe_a", "Fraktion/Gruppe_b"]
    )
    assert all([v in res.columns for v in expected_columns])


def test_get_party_party_simlarity():
    similarity_party_party = pd.DataFrame(
        {
            "Fraktion/Gruppe_a": ["A", "A", "A", "A", "A"],
            "Fraktion/Gruppe_b": ["B", "C", "D", "E", "F"],
            "similarity": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )

    # line to test
    res = sim.get_party_party_similarity(similarity_party_party)

    assert len(res) == similarity_party_party["Fraktion/Gruppe_b"].nunique()
    assert all([v in res.columns for v in ["mean", "50%"]])
    assert res["mean"].between(0, 1).all()
    assert sorted(res.index.values) == sorted(
        similarity_party_party["Fraktion/Gruppe_b"].unique()
    )


def test_plot_overall_similarity(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)
    mdb_votes = sim.prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = sim.align_mdb_with_parties(mdb_votes, pivoted).pipe(
        sim.compute_similarity, lsuffix="mdb", rsuffix="party"
    )

    # line to test
    ax = sim.plot_overall_similarity(
        mdb_vs_parties, x="Fraktion/Gruppe", title="bla"
    )

    assert isinstance(ax, plt.Axes)


def test_plot_similarity_over_time(df: pd.DataFrame):
    votes = sim.get_votes_by_party(df)
    pivoted = sim.pivot_party_votes_df(votes)
    mdb_votes = sim.prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = sim.align_mdb_with_parties(mdb_votes, pivoted).pipe(
        sim.compute_similarity, lsuffix="mdb", rsuffix="party"
    )
    mdb_vs_parties["date"] = pd.to_datetime(mdb_vs_parties["date"])
    # line to test
    ax = sim.plot_similarity_over_time(
        mdb_vs_parties, "Fraktion/Gruppe", title="bla"
    )

    assert isinstance(ax, plt.Axes)


def test_plot():
    df = pd.DataFrame({"a": [1]})

    with (
        patch(
            "bundestag.similarity.plot_overall_similarity", MagicMock()
        ) as _plot_overall,
        patch(
            "bundestag.similarity.plot_similarity_over_time", MagicMock()
        ) as _plot_time,
    ):
        # line to test
        axs = sim.plot(df, "a", "b", party_col="bla")

        assert isinstance(axs, np.ndarray)
        assert all(isinstance(v, plt.Axes) for v in axs.flatten())
        _plot_overall.assert_called_once()
        _plot_time.assert_called_once()
