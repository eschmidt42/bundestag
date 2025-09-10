from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import polars as pl
import pytest

import bundestag.data.transform.bundestag_sheets as transform_bs
from bundestag.ml.similarity import (
    align_mdb_with_parties,
    align_party_with_all_parties,
    align_party_with_party,
    compute_similarity,
    cosine_similarity,
    get_party_party_similarity,
    get_votes_by_party,
    pivot_party_votes_df,
    plot,
    plot_overall_similarity,
    plot_similarity_over_time,
    prepare_votes_of_mdb,
)


@pytest.fixture(scope="module")
def df():
    return pl.DataFrame(
        {
            "Fraktion/Gruppe": ["A", "A", "A", "B", "B", "B"],
            "vote": ["ja", "nein", "Enthaltung", "ja", "nein", "Enthaltung"],
            "date": ["2022-02-02"] * 6,
            "title": ["bla"] * 6,
            "Bezeichnung": ["A", "B", "C", "D", "E", "F"],
        }
    )


def test_votes_by_party(df: pl.DataFrame):
    # line to test
    votes = get_votes_by_party(df)

    expected_columns = [
        "Fraktion/Gruppe",
        "date",
        "title",
        "fraction",
        "vote",
        "# votes",
    ]
    assert all([v in votes.columns for v in expected_columns])
    assert votes["# votes"].is_null().sum() == 0


def test_pivot_party_votes_df(df: pl.DataFrame):
    votes = get_votes_by_party(df)

    # line to test
    pivoted = pivot_party_votes_df(votes)

    vote_columns = ["ja", "nein", "Enthaltung"]
    other_columns = ["Fraktion/Gruppe"]
    assert all([v in pivoted.columns for v in vote_columns + other_columns])
    for c in vote_columns:
        assert pivoted[c].is_null().sum() == 0


@pytest.mark.parametrize("mdb", ["A", "wup"])
def test_prepare_votes_of_mdb(mdb: str, df: pl.DataFrame):
    try:
        # line to test
        mdb_votes = prepare_votes_of_mdb(df, mdb)
    except ValueError as ex:
        if mdb == "wup":
            pytest.xfail("mdb not found")
        else:
            raise ex

    expected_columns = ["date", "title"] + transform_bs.VOTE_COLS
    assert all([v in mdb_votes.columns for v in expected_columns])
    for c in transform_bs.VOTE_COLS:
        assert mdb_votes[c].is_null().sum() == 0


def test_align_mdb_with_parties(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)
    mdb_votes = prepare_votes_of_mdb(df, "A")

    # line to test
    mdb_vs_parties = align_mdb_with_parties(mdb_votes, pivoted)

    expected_columns = (
        ["date", "title"]
        + [v for v in transform_bs.VOTE_COLS]
        + [f"{v}_party" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe"]
    )
    for v in expected_columns:
        assert v in mdb_vs_parties.columns


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
    res = cosine_similarity(a, b)

    assert np.isclose(res, expected)


def test_compute_similarity(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)
    mdb_votes = prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = align_mdb_with_parties(mdb_votes, pivoted)

    # line to test
    res = compute_similarity(mdb_vs_parties, suffix="_party")

    assert all([v in res.columns for v in mdb_vs_parties.columns])
    assert "similarity" in res.columns
    assert res["similarity"].is_between(0, 1).all()


def test_align_party_with_party(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)

    # line to test
    res = align_party_with_party(pivoted, party_a="A", party_b="B")

    expected_columns = (
        ["date", "title"]
        + [v for v in transform_bs.VOTE_COLS]
        + [f"{v}_b" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe", "Fraktion/Gruppe_b"]
    )
    assert all([v in res.columns for v in expected_columns])


def test_align_party_with_all_parties(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)

    # line to test
    res = align_party_with_all_parties(pivoted, party_a="A")

    expected_columns = (
        ["date", "title"]
        + [v for v in transform_bs.VOTE_COLS]
        + [f"{v}_b" for v in transform_bs.VOTE_COLS]
        + ["Fraktion/Gruppe", "Fraktion/Gruppe_b"]
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
    res = get_party_party_similarity(similarity_party_party)

    assert len(res) == similarity_party_party["Fraktion/Gruppe_b"].nunique()
    assert all([v in res.columns for v in ["mean", "50%"]])
    assert res["mean"].between(0, 1).all()
    assert sorted(res.index.values) == sorted(
        similarity_party_party["Fraktion/Gruppe_b"].unique()
    )


@pytest.mark.skip(
    "Works but something to be run elsewhere, not as a unit test that opens a window."
)
def test_plot_overall_similarity(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)
    mdb_votes = prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = align_mdb_with_parties(mdb_votes, pivoted)
    mdb_vs_parties = compute_similarity(mdb_vs_parties, suffix="_party")

    # line to test
    ax = plot_overall_similarity(
        mdb_vs_parties,
        x="Fraktion/Gruppe",
        title="bla",
        palette={"A": "green", "B": "blue"},
    )


@pytest.mark.skip(
    "Works but something to be run elsewhere, not as a unit test that opens a window."
)
def test_plot_similarity_over_time(df: pl.DataFrame):
    votes = get_votes_by_party(df)
    pivoted = pivot_party_votes_df(votes)
    mdb_votes = prepare_votes_of_mdb(df, "A")
    mdb_vs_parties = align_mdb_with_parties(mdb_votes, pivoted)
    mdb_vs_parties = compute_similarity(mdb_vs_parties, suffix="_party")

    # line to test
    ax = plot_similarity_over_time(
        mdb_vs_parties,
        "Fraktion/Gruppe",
        title="bla",
        palette={"A": "green", "B": "blue"},
    )


@pytest.mark.skip(
    "Works but something to be run elsewhere, not as a unit test that opens a window."
)
def test_plot():
    df = pl.DataFrame({"a": [1]})

    with (
        patch(
            "bundestag.similarity.plot_overall_similarity", MagicMock()
        ) as _plot_overall,
        patch(
            "bundestag.similarity.plot_similarity_over_time", MagicMock()
        ) as _plot_time,
    ):
        # line to test
        axs = plot(df, "a", "b", party_col="bla")

        assert isinstance(axs, np.ndarray)
        _plot_overall.assert_called_once()
        _plot_time.assert_called_once()
