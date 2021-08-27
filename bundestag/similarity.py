import sys
import pandas as pd
import plotly.express as px
from scipy import spatial
from bundestag import html_parsing as hp
from loguru import logger
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

logger.remove()
logger.add(sys.stderr, level="INFO")


def get_votes_by_party(df: pd.DataFrame):
    "Computes total and relative votes by party and poll"
    logger.info("Computing votes by party and poll")
    df["vote"] = df["vote"].astype("category")
    df["vote"] = df["vote"].cat.set_categories(hp.VOTE_COLS)

    vote_fraction = (
        df.groupby(["Fraktion/Gruppe", "date", "title"])["vote"]
        .value_counts(normalize=True)
        .to_frame("fraction")
    )
    vote_count = (
        df.groupby(["Fraktion/Gruppe", "date", "title"])["vote"]
        .value_counts(normalize=True)
        .to_frame("# votes")
    )

    votes = (
        vote_fraction.join(vote_count).reset_index().rename(columns={"level_3": "vote"})
    )
    return votes


def test_party_votes(df: pd.DataFrame):
    expected_columns = [
        "Fraktion/Gruppe",
        "date",
        "title",
        "fraction",
        "vote",
        "# votes",
    ]
    assert all([v in df.columns for v in expected_columns])
    assert df["# votes"].isna().sum() == 0


def pivot_party_votes_df(df: pd.DataFrame):
    return df.pivot_table(
        index=["Fraktion/Gruppe", "date", "title"],
        columns="vote",
        values="fraction",
        fill_value=0,
    ).reset_index(level="Fraktion/Gruppe")


def test_party_votes_pivoted(df: pd.DataFrame):
    assert np.allclose(
        df[["ja", "nein", "Enthaltung", "ungültig", "nichtabgegeben"]].sum(axis=1), 1
    )


def prepare_votes_of_mdb(df: pd.DataFrame, mdb: str):
    assert mdb in df["Bezeichnung"].unique()
    mask = df["Bezeichnung"] == mdb

    mdb_votes = df.loc[mask, ["date", "title", "vote"]]
    mdb_votes["vote"] = mdb_votes["vote"].astype("category")
    mdb_votes["vote"] = mdb_votes["vote"].cat.set_categories(hp.VOTE_COLS)

    mdb_votes = pd.get_dummies(mdb_votes, columns=["vote"], prefix="", prefix_sep="")
    return mdb_votes


def test_votes_of_mdb(df: pd.DataFrame):
    expected_columns = ["date", "title"] + hp.VOTE_COLS
    assert all([v in df.columns for v in expected_columns])
    assert df[hp.VOTE_COLS].isna().sum().sum() == 0


def align_mdb_with_parties(mdb_votes: pd.DataFrame, party_votes_pivoted: pd.DataFrame):
    return mdb_votes.join(
        party_votes_pivoted,
        on=["date", "title"],
        lsuffix="_mdb",
        rsuffix="_party",
    )


def cosine_similarity(a, b):
    return 1 - spatial.distance.cosine(a, b)


def compute_similarity(
    df: pd.DataFrame,
    lsuffix: str,
    rsuffix: str,
    similarity_metric=cosine_similarity,
):
    """Computes similarities based on `lsuffix` and `rsuffix` using the metric passed in `similarity_metric`.

    The idea: Interpret the n polls (rows in `df`) with their "yes"/"no"/"invalid"/"abstain"/"not handed in" outcomes
    as an n-by-5 matrix. `lsuffix` indicates the n-by-5 matrix for party/politician A and `rsuffix`
    idnicates the n-by-5 matrix for party/politician B. Use each row to compute a similarity score between
    A and B and return this as "similarity".
    """
    logger.info(
        f'Computing similarities using `lsuffix` = "{lsuffix}", `rsuffix` = "{rsuffix}" and metric = {similarity_metric}'
    )
    lcols = [f"{v}_{lsuffix}" for v in hp.VOTE_COLS]
    rcols = [f"{v}_{rsuffix}" for v in hp.VOTE_COLS]
    A = df[lcols].values
    B = df[rcols].values
    df["similarity"] = [similarity_metric(a, b) for a, b in zip(A, B)]
    return df


def test_mdb_vs_parties(df: pd.DataFrame):
    mdb_cols = [f"{v}_mdb" for v in hp.VOTE_COLS]
    party_cols = [f"{v}_party" for v in hp.VOTE_COLS]
    expected_columns = (
        ["date", "title", "similarity", "Fraktion/Gruppe"] + mdb_cols + party_cols
    )
    assert all([v in df.columns for v in expected_columns])
    assert np.allclose(df[mdb_cols].sum(axis=1), 1)
    assert np.allclose(df[party_cols].sum(axis=1), 1)
    assert ((df["similarity"] >= 0) | (df["similarity"] <= 1)).all()


def get_mdb_party_similarity(similarity_mdb_party: pd.DataFrame):
    return (
        similarity_mdb_party.groupby("Fraktion/Gruppe")["similarity"]
        .describe()
        .sort_values("mean")
    )


def align_party_with_party(party_votes: pd.DataFrame, party_a: str, party_b: str):

    tmp = party_votes.copy()
    mask_a = tmp["Fraktion/Gruppe"] == party_a
    mask_b = tmp["Fraktion/Gruppe"] == party_b
    return (
        tmp.loc[mask_a].join(tmp.loc[mask_b], lsuffix="_a", rsuffix="_b").reset_index()
    )


def align_party_with_all_parties(party_votes: pd.DataFrame, party_a: str):
    partyA_vs_rest = []
    parties = [p for p in party_votes["Fraktion/Gruppe"].unique() if p != party_a]
    for party_b in parties:
        tmp = align_party_with_party(party_votes, party_a=party_a, party_b=party_b)
        partyA_vs_rest.append(tmp)
    partyA_vs_rest = pd.concat(partyA_vs_rest, ignore_index=True)
    notna = partyA_vs_rest["Fraktion/Gruppe_b"].notna()
    return partyA_vs_rest.loc[notna]


def test_partyA_vs_partyB(df: pd.DataFrame):

    partyA_cols = [f"{v}_a" for v in hp.VOTE_COLS]
    partyB_cols = [f"{v}_b" for v in hp.VOTE_COLS]
    expected_columns = (
        ["date", "title", "similarity", "Fraktion/Gruppe_a", "Fraktion/Gruppe_b"]
        + partyA_cols
        + partyB_cols
    )
    assert all([v in df.columns for v in expected_columns])
    assert np.allclose(df[partyA_cols].sum(axis=1), 1)
    assert np.allclose(df[partyB_cols].sum(axis=1), 1)
    assert ((df["similarity"] >= 0) | (df["similarity"] <= 1)).all()


def get_party_party_similarity(similarity_party_party: pd.DataFrame):
    return (
        similarity_party_party.groupby("Fraktion/Gruppe_b")["similarity"]
        .describe()
        .sort_values("mean")
    )


def plot_overall_similarity(df: pd.DataFrame, x: str, title: str = "", ax=None):
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))
    sns.stripplot(data=df, y="similarity", x=x, ax=ax, alpha=0.1)
    ax.set(title=title, ylabel="Similarity (1 = identical, 0 = dissimilar)")
    return ax
    # plt.tight_layout()
    # plt.show()


def plot_similarity_over_time(
    df: pd.DataFrame, grp_col: str, time_bin: str = "y", title: str = None, ax=None
):
    y = "avg. similarity"
    tmp = (
        df.groupby([pd.Grouper(key="date", freq=time_bin), grp_col])["similarity"]
        .mean()
        .to_frame(y)
        .reset_index()
    )
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))
    sns.lineplot(data=tmp, x="date", y=y, hue=grp_col, ax=ax)
    ax.set(
        xlabel=f"Time [{time_bin}]",
        ylabel=f"{y} (0 = dissimilar, 1 = identical)",
        title=title,
    )
    return ax
    # plt.tight_layout()
    # plt.show()
    # fig = px.line(data_frame=tmp, x="date", y=y, color=grp_col, title=title)
    # fig.update_layout(
    #     xaxis_title=f"Time [{time_bin}]",
    #     yaxis_title=f"{y} (0 = dissimilar, 1 = identical)",
    # )
    # return fig


def plot(
    df: pd.DataFrame,
    title_overall: str = "",
    title_over_time: str = "",
    party_col: str = "Fraktion/Gruppe",
):
    fig, axs = plt.subplots(figsize=(12, 8), nrows=2)
    plot_overall_similarity(df, x=party_col, title=title_overall, ax=axs[0])
    plot_similarity_over_time(df, party_col, title=title_over_time, ax=axs[1])
    return axs
