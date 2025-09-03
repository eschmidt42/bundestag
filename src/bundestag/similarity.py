import logging
from typing import Any, Callable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
from scipy import spatial

import bundestag.data.transform.bundestag_sheets as transform_bs

logger = logging.getLogger(__name__)


def get_votes_by_party(df: pl.DataFrame) -> pl.DataFrame:
    "Computes total and relative votes by party and poll"

    logger.info("Computing votes by party and poll")
    df = df.with_columns(
        **{"vote": pl.col("vote").cast(pl.Enum(transform_bs.VOTE_COLS))}
    )

    vote_fraction = (
        df.group_by(["Fraktion/Gruppe", "date", "title"])
        .agg(pl.col("vote").value_counts(normalize=True, name="fraction"))
        .explode("vote")
        .unnest("vote")
    )
    vote_count = (
        df.group_by(["Fraktion/Gruppe", "date", "title"])
        .agg(pl.col("vote").value_counts(name="# votes"))
        .explode("vote")
        .unnest("vote")
    )

    votes = vote_fraction.join(
        vote_count, on=["Fraktion/Gruppe", "date", "title", "vote"]
    )

    return votes


def assign_missing_vote_columns(df: pl.DataFrame) -> pl.DataFrame:
    missing_vote_outcomes = [c for c in transform_bs.VOTE_COLS if c not in df.columns]
    if len(missing_vote_outcomes) > 0:
        df = df.with_columns(**{c: pl.lit(None) for c in missing_vote_outcomes})

    return df


def fill_vote_columns(df: pl.DataFrame, fill_value: float | int) -> pl.DataFrame:
    df = df.with_columns(
        **{c: pl.col(c).fill_null(fill_value) for c in transform_bs.VOTE_COLS}
    )

    return df


def pivot_party_votes_df(df: pl.DataFrame) -> pl.DataFrame:
    pivoted = df.pivot(
        index=["Fraktion/Gruppe", "date", "title"],
        on="vote",
        values="fraction",
    )
    pivoted = assign_missing_vote_columns(pivoted)
    pivoted = fill_vote_columns(pivoted, 0.0)

    return pivoted


def prepare_votes_of_mdb(df: pl.DataFrame, mdb: str) -> pl.DataFrame:
    mdbs = df["Bezeichnung"].unique().to_list()
    if not mdb in mdbs:
        raise ValueError(f"{mdb} not found in column 'Bezeichnung'")

    mdb_votes = (
        df.filter(pl.col("Bezeichnung").eq(pl.lit(mdb)))
        .with_columns(**{"vote": pl.col("vote").cast(pl.Enum(transform_bs.VOTE_COLS))})
        .to_dummies(["vote"])
    )

    cols_map = {c: c.split("_")[1] for c in mdb_votes.columns if c.startswith("vote_")}

    mdb_votes = mdb_votes.rename(cols_map)

    mdb_votes = assign_missing_vote_columns(mdb_votes)
    mdb_votes = fill_vote_columns(mdb_votes, 0)

    return mdb_votes


def align_mdb_with_parties(
    mdb_votes: pl.DataFrame, party_votes_pivoted: pl.DataFrame
) -> pl.DataFrame:
    return mdb_votes.join(party_votes_pivoted, on=["date", "title"], suffix="_party")


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    "Computes the cosine similarity between two vectors, i.e. perpendicular vectors have a similarity of zero and parallel vectors similarity of one."
    return float(1 - spatial.distance.cosine(a, b))


def compute_similarity(
    df: pl.DataFrame,
    suffix: str,
    similarity_metric: Callable = cosine_similarity,
) -> pl.DataFrame:
    """Computes similarities based on `lsuffix` and `rsuffix` using the metric passed in `similarity_metric`.

    The idea: Interpret the n polls (rows in `df`) with their "yes"/"no"/"invalid"/"abstain"/"not handed in" outcomes
    as an n-by-5 matrix. `lsuffix` indicates the n-by-5 matrix for party/politician A and `rsuffix`
    idnicates the n-by-5 matrix for party/politician B. Use each row to compute a similarity score between
    A and B and return this as "similarity".
    """
    logger.info(f"Computing similarities using metric = {similarity_metric}")
    lcols = transform_bs.VOTE_COLS
    rcols = [f"{v}{suffix}" for v in transform_bs.VOTE_COLS]

    A = df.select(lcols).to_numpy()
    B = df.select(rcols).to_numpy()
    s = [similarity_metric(a, b) for a, b in zip(A, B, strict=True)]
    df = df.with_columns(**{"similarity": pl.Series(s)})

    return df


def align_party_with_party(
    party_votes: pl.DataFrame, party_a: str, party_b: str
) -> pl.DataFrame:
    col = "Fraktion/Gruppe"
    votes_party_a = party_votes.filter(pl.col(col).eq(party_a))
    votes_party_b = party_votes.filter(pl.col(col).eq(party_b))

    aligned_parties = votes_party_a.join(
        votes_party_b, on=["date", "title"], suffix="_b"
    )
    return aligned_parties


def align_party_with_all_parties(
    party_votes: pl.DataFrame, party_a: str
) -> pl.DataFrame:
    partyA_vs_rest = []
    parties = [p for p in party_votes["Fraktion/Gruppe"].unique() if p != party_a]

    for party_b in parties:
        tmp = align_party_with_party(party_votes, party_a=party_a, party_b=party_b)
        partyA_vs_rest.append(tmp)

    partyA_vs_rest = pl.concat(partyA_vs_rest)
    return partyA_vs_rest


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
    df: pl.DataFrame,
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
    df: pl.DataFrame,
    party_col: str,
    title: str,
    ax: Any | None = None,
    palette: dict[str, str] | None = None,
) -> Any:
    y = "avg. similarity"
    tmp = df.with_columns(**{"year": pl.col("date").dt.year()})
    tmp = tmp.group_by(["year", party_col]).agg(**{y: pl.col("similarity").mean()})
    palette = PALETTE if palette is None else palette
    if ax is None:
        fig, ax = plt.subplots(figsize=(12, 4))

    sns.lineplot(data=tmp, x="year", y=y, hue=party_col, ax=ax, palette=palette)

    ax.set(
        xlabel=f"Year",
        ylabel=f"{y} (0 = dissimilar, 1 = identical)",
        title=title,
    )
    return ax


def plot(
    df: pl.DataFrame,
    title_overall: str = "",
    title_over_time: str = "",
    party_col: str = "Fraktion/Gruppe_party",
) -> np.ndarray:
    fig, axs = plt.subplots(figsize=(12, 8), nrows=2)
    plot_overall_similarity(df, x=party_col, title=title_overall, ax=axs[0])
    plot_similarity_over_time(df, party_col, title=title_over_time, ax=axs[1])
    return axs
