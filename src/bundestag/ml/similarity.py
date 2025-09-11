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
    """Computes the total and relative votes by party for each poll.

    This function groups the input DataFrame by party, date, and poll title,
    and then calculates both the absolute count and the fraction of each vote type
    ('ja', 'nein', etc.) within each group.

    Args:
        df (pl.DataFrame): The input DataFrame containing individual vote records.

    Returns:
        pl.DataFrame: A DataFrame with vote counts and fractions for each party and poll.
    """

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
    """Ensures that all standard vote columns exist in the DataFrame.

    This function checks if any of the standard vote columns (from `transform_bs.VOTE_COLS`)
    are missing from the DataFrame and adds them with `None` as the value if they are.

    Args:
        df (pl.DataFrame): The DataFrame to check and modify.

    Returns:
        pl.DataFrame: The DataFrame with all standard vote columns present.
    """
    missing_vote_outcomes = [c for c in transform_bs.VOTE_COLS if c not in df.columns]
    if len(missing_vote_outcomes) > 0:
        df = df.with_columns(**{c: pl.lit(None) for c in missing_vote_outcomes})

    return df


def fill_vote_columns(df: pl.DataFrame, fill_value: float | int) -> pl.DataFrame:
    """Fills null values in the standard vote columns with a specified value.

    Args:
        df (pl.DataFrame): The DataFrame containing vote columns.
        fill_value (float | int): The value to use for filling nulls.

    Returns:
        pl.DataFrame: The DataFrame with nulls in vote columns filled.
    """
    df = df.with_columns(
        **{c: pl.col(c).fill_null(fill_value) for c in transform_bs.VOTE_COLS}
    )

    return df


def pivot_party_votes_df(df: pl.DataFrame) -> pl.DataFrame:
    """Pivots the party votes DataFrame to a wide format.

    This function transforms a long-format DataFrame of party votes into a wide format,
    where each vote type becomes a separate column. It also ensures all vote columns
    exist and fills any null values with 0.0.

    Args:
        df (pl.DataFrame): The long-format DataFrame of party votes.

    Returns:
        pl.DataFrame: The pivoted, wide-format DataFrame.
    """
    pivoted = df.pivot(
        index=["Fraktion/Gruppe", "date", "title"],
        on="vote",
        values="fraction",
    )
    pivoted = assign_missing_vote_columns(pivoted)
    pivoted = fill_vote_columns(pivoted, 0.0)

    return pivoted


def prepare_votes_of_mdb(df: pl.DataFrame, mdb: str) -> pl.DataFrame:
    """Prepares the voting data for a single Member of Parliament (MdB).

    This function filters the main votes DataFrame for a specific MdB,
    converts their single 'vote' column into a one-hot encoded format (dummy variables),
    and ensures all standard vote columns are present and filled.

    Args:
        df (pl.DataFrame): The main DataFrame containing all votes.
        mdb (str): The identifier for the Member of Parliament.

    Raises:
        ValueError: If the specified `mdb` is not found in the DataFrame.

    Returns:
        pl.DataFrame: A DataFrame containing the one-hot encoded votes for the specified MdB.
    """
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
    """Aligns the votes of an MdB with the aggregated votes of parties.

    This function joins the MdB's voting record with the pivoted party voting records
    on the date and title of the poll, allowing for direct comparison.

    Args:
        mdb_votes (pl.DataFrame): The prepared voting data for the MdB.
        party_votes_pivoted (pl.DataFrame): The pivoted, wide-format party voting data.

    Returns:
        pl.DataFrame: A DataFrame with the MdB's and parties' votes aligned for each poll.
    """
    return mdb_votes.join(party_votes_pivoted, on=["date", "title"], suffix="_party")


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Computes the cosine similarity between two vectors.

    Perpendicular vectors will have a similarity of 0, and parallel vectors will have a similarity of 1.

    Args:
        a (np.ndarray): The first vector.
        b (np.ndarray): The second vector.

    Returns:
        float: The cosine similarity between the two vectors.
    """

    return float(1 - spatial.distance.cosine(a, b))


def compute_similarity(
    df: pl.DataFrame,
    suffix: str,
    similarity_metric: Callable = cosine_similarity,
) -> pl.DataFrame:
    """Computes the similarity between two sets of vote vectors in a DataFrame.

    This function treats the vote outcomes for each poll as vectors. It compares the vectors
    from the base columns (`transform_bs.VOTE_COLS`) with the vectors from the columns
    identified by a suffix (e.g., 'ja_party', 'nein_party').

    Args:
        df (pl.DataFrame): The DataFrame containing the aligned vote vectors.
        suffix (str): The suffix used to identify the second set of vote columns (e.g., '_party').
        similarity_metric (Callable, optional): The function to use for calculating similarity. Defaults to `cosine_similarity`.

    Returns:
        pl.DataFrame: The input DataFrame with an added 'similarity' column.
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
    """Aligns the voting records of two parties for comparison.

    This function filters the party votes DataFrame for two specified parties
    and joins their records on the poll's date and title.

    Args:
        party_votes (pl.DataFrame): The DataFrame containing pivoted party votes.
        party_a (str): The name of the first party.
        party_b (str): The name of the second party.

    Returns:
        pl.DataFrame: A DataFrame with the voting records of the two parties aligned.
    """
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
    """Aligns the voting record of one party with all other parties.

    Args:
        party_votes (pl.DataFrame): The DataFrame containing pivoted party votes.
        party_a (str): The name of the party to compare against all others.

    Returns:
        pl.DataFrame: A concatenated DataFrame containing the alignment of `party_a` with every other party.
    """
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
    """Calculates descriptive statistics for party-party similarity scores.

    Args:
        similarity_party_party (pd.DataFrame): A DataFrame containing similarity scores between pairs of parties.

    Returns:
        pd.DataFrame: A DataFrame with summary statistics (mean, std, etc.) of the similarity scores, grouped by the second party.
    """
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
    """Creates a strip plot to visualize the overall distribution of similarity scores.

    Args:
        df (pl.DataFrame): The DataFrame containing the similarity data.
        x (str): The column to be used for the x-axis (typically the party name).
        title (str, optional): The title of the plot. Defaults to "".
        ax (matplotlib.axes.Axes, optional): The matplotlib axes object to plot on. If None, a new figure and axes are created. Defaults to None.
        palette (dict[str, str] | None, optional): A color palette for the plot. Defaults to a predefined palette.

    Returns:
        matplotlib.axes.Axes: The axes object with the plot.
    """
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
    """Creates a line plot to visualize how similarity scores change over time.

    This function aggregates the similarity scores by year and party and plots the average similarity.

    Args:
        df (pl.DataFrame): The DataFrame containing the similarity data with a 'date' column.
        party_col (str): The column containing the party names.
        title (str): The title of the plot.
        ax (matplotlib.axes.Axes, optional): The matplotlib axes object to plot on. If None, a new figure and axes are created. Defaults to None.
        palette (dict[str, str] | None, optional): A color palette for the plot. Defaults to a predefined palette.

    Returns:
        matplotlib.axes.Axes: The axes object with the plot.
    """
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
    """Creates a combined plot showing both overall similarity and similarity over time.

    Args:
        df (pl.DataFrame): The DataFrame containing the similarity data.
        title_overall (str, optional): The title for the overall similarity plot. Defaults to "".
        title_over_time (str, optional): The title for the similarity over time plot. Defaults to "".
        party_col (str, optional): The column containing the party names. Defaults to "Fraktion/Gruppe_party".

    Returns:
        np.ndarray: An array of the matplotlib axes objects for the two subplots.
    """
    fig, axs = plt.subplots(figsize=(12, 8), nrows=2)
    plot_overall_similarity(df, x=party_col, title=title_overall, ax=axs[0])
    plot_similarity_over_time(df, party_col, title=title_over_time, ax=axs[1])
    return axs
