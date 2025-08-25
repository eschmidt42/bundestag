import logging
from typing import Any, Callable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy import spatial

import bundestag.data.transform.bundestag_sheets as transform_bs

logger = logging.getLogger(__name__)


def get_votes_by_party(df: pd.DataFrame) -> pd.DataFrame:
    "Computes total and relative votes by party and poll"

    logger.info("Computing votes by party and poll")
    df["vote"] = df["vote"].astype("category")
    df["vote"] = df["vote"].cat.set_categories(transform_bs.VOTE_COLS)

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


def pivot_party_votes_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.pivot_table(
        index=["Fraktion/Gruppe", "date", "title"],
        columns="vote",
        values="fraction",
        fill_value=0,
    ).reset_index(level="Fraktion/Gruppe")


def prepare_votes_of_mdb(df: pd.DataFrame, mdb: str) -> pd.DataFrame:
    if not mdb in df["Bezeichnung"].unique():
        raise ValueError(f"{mdb} not found in column 'Bezeichnung'")
    mask = df["Bezeichnung"] == mdb

    mdb_votes = df.loc[mask, ["date", "title", "vote"]]
    mdb_votes["vote"] = mdb_votes["vote"].astype("category")
    mdb_votes["vote"] = mdb_votes["vote"].cat.set_categories(transform_bs.VOTE_COLS)

    mdb_votes = pd.get_dummies(mdb_votes, columns=["vote"], prefix="", prefix_sep="")
    return mdb_votes


def align_mdb_with_parties(
    mdb_votes: pd.DataFrame, party_votes_pivoted: pd.DataFrame
) -> pd.DataFrame:
    return mdb_votes.join(
        party_votes_pivoted,
        on=["date", "title"],
        lsuffix="_mdb",
        rsuffix="_party",
    )


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    "Computes the cosine similarity between two vectors, i.e. perpendicular vectors have a similarity of zero and parallel vectors similarity of one."
    return float(1 - spatial.distance.cosine(a, b))


def compute_similarity(
    df: pd.DataFrame,
    lsuffix: str,
    rsuffix: str,
    similarity_metric: Callable = cosine_similarity,
) -> pd.DataFrame:
    """Computes similarities based on `lsuffix` and `rsuffix` using the metric passed in `similarity_metric`.

    The idea: Interpret the n polls (rows in `df`) with their "yes"/"no"/"invalid"/"abstain"/"not handed in" outcomes
    as an n-by-5 matrix. `lsuffix` indicates the n-by-5 matrix for party/politician A and `rsuffix`
    idnicates the n-by-5 matrix for party/politician B. Use each row to compute a similarity score between
    A and B and return this as "similarity".
    """
    logger.info(
        f'Computing similarities using `lsuffix` = "{lsuffix}", `rsuffix` = "{rsuffix}" and metric = {similarity_metric}'
    )
    lcols = [f"{v}_{lsuffix}" for v in transform_bs.VOTE_COLS]
    rcols = [f"{v}_{rsuffix}" for v in transform_bs.VOTE_COLS]
    A = df[lcols].values
    B = df[rcols].values
    df["similarity"] = [similarity_metric(a, b) for a, b in zip(A, B)]
    return df


def align_party_with_party(
    party_votes: pd.DataFrame, party_a: str, party_b: str
) -> pd.DataFrame:
    tmp = party_votes.copy()
    mask_a = tmp["Fraktion/Gruppe"] == party_a
    mask_b = tmp["Fraktion/Gruppe"] == party_b
    return (
        tmp.loc[mask_a].join(tmp.loc[mask_b], lsuffix="_a", rsuffix="_b").reset_index()
    )


def align_party_with_all_parties(
    party_votes: pd.DataFrame, party_a: str
) -> pd.DataFrame:
    partyA_vs_rest = []
    parties = [p for p in party_votes["Fraktion/Gruppe"].unique() if p != party_a]
    for party_b in parties:
        tmp = align_party_with_party(party_votes, party_a=party_a, party_b=party_b)
        partyA_vs_rest.append(tmp)
    partyA_vs_rest = pd.concat(partyA_vs_rest, ignore_index=True)
    notna = partyA_vs_rest["Fraktion/Gruppe_b"].notna()
    return partyA_vs_rest.loc[notna]


def get_party_party_similarity(
    similarity_party_party: pd.DataFrame,
) -> pd.DataFrame:
    return (
        similarity_party_party.groupby("Fraktion/Gruppe_b")["similarity"]
        .describe()
        .sort_values("mean")
    )


PALETTE = {
    "AfD": "blue",
    "CDU/CSU": "black",
    "FDP": "yellow",
    "BÜ90/GR": "green",
    "DIE LINKE.": "purple",
    "Die Linke": "purple",
    "SPD": "red",
    "Fraktionslos": "grey",
    "BSW": "magenta",
}


def plot_overall_similarity(
    df: pd.DataFrame,
    x: str,
    title: str = "",
    ax=None,
    palette: dict[str, str] | None = None,
) -> Any:
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))
    palette = PALETTE if palette is None else palette
    sns.stripplot(
        data=df, y="similarity", x=x, ax=ax, alpha=0.1, hue=x, palette=palette
    )
    ax.set(title=title, ylabel="Similarity (1 = identical, 0 = dissimilar)")
    return ax


def plot_similarity_over_time(
    df: pd.DataFrame,
    party_col: str,
    title: str,
    time_bin: str = "y",
    ax: Any | None = None,
    palette: dict[str, str] | None = None,
) -> Any:
    y = "avg. similarity"
    tmp = (
        df.groupby([pd.Grouper(key="date", freq=time_bin), party_col])["similarity"]
        .mean()
        .to_frame(y)
        .reset_index()
    )
    palette = PALETTE if palette is None else palette
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))
    sns.lineplot(data=tmp, x="date", y=y, hue=party_col, ax=ax, palette=palette)
    ax.set(
        xlabel=f"Time [{time_bin}]",
        ylabel=f"{y} (0 = dissimilar, 1 = identical)",
        title=title,
    )
    return ax


def plot(
    df: pd.DataFrame,
    title_overall: str = "",
    title_over_time: str = "",
    party_col: str = "Fraktion/Gruppe",
) -> np.ndarray:
    fig, axs = plt.subplots(figsize=(12, 8), nrows=2)
    plot_overall_similarity(df, x=party_col, title=title_overall, ax=axs[0])
    plot_similarity_over_time(df, party_col, title=title_over_time, ax=axs[1])
    return axs
