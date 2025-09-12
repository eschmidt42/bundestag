import logging

import numpy as np
import pandas as pd
import polars as pl
import torch
from fastai.tabular.all import TabularLearner
from plotnine import aes, geom_point, ggplot, labs, scale_color_manual
from sklearn import decomposition

logger = logging.getLogger(__name__)

PALETTE = {
    "CDU/CSU": "black",
    "FDP": "yellow",
    "SPD": "red",
    "DIE GRÜNEN": "green",
    "AfD": "blue",
    "DIE LINKE": "purple",
}


def poll_splitter(
    df: pl.DataFrame,
    poll_col: str = "poll_id",
    valid_pct: float = 0.2,
    shuffle: bool = True,
    seed: int | None = None,
) -> tuple[list[int], list[int]]:
    """Splits the DataFrame into training and validation sets based on poll IDs.

    This function ensures that all votes for a given poll are in the same set (either training or validation),
    preventing data leakage between the sets.

    Args:
        df (pl.DataFrame): The DataFrame to be split.
        poll_col (str, optional): The name of the column containing poll IDs. Defaults to "poll_id".
        valid_pct (float, optional): The fraction of polls to be used for the validation set. Defaults to 0.2.
        shuffle (bool, optional): Whether to shuffle the indices within the train and validation sets. Defaults to True.
        seed (int | None, optional): A seed for the random number generator for reproducibility. Defaults to None.

    Returns:
        tuple[list[int], list[int]]: A tuple containing two lists of indices: the first for the training set,
                                     and the second for the validation set.
    """

    polls = df[poll_col].unique()
    n = len(polls)

    if seed is None:
        rng = np.random.RandomState()
    else:
        rng = np.random.RandomState(seed)

    polls1 = rng.choice(polls, size=int(n * valid_pct))

    logger.debug(
        f"Splitting votes by polls (num train = {n - len(polls1)}, num valid = {len(polls1)})"
    )

    df = df.with_columns(
        **{"is validation set": pl.col(poll_col).is_in(list(polls1))}
    ).with_row_index(name="index")

    ix0 = df.filter(pl.col("is validation set").not_())["index"].to_list()
    ix1 = df.filter(pl.col("is validation set"))["index"].to_list()

    if shuffle:
        rng.shuffle(ix0)
        rng.shuffle(ix1)

    return (ix0, ix1)


def plot_predictions(
    learn: TabularLearner,
    df_all_votes: pl.DataFrame,
    df_mandates: pl.DataFrame,
    df_polls: pl.DataFrame,
    splits: tuple[list[int], list[int]],
    y_col: str = "vote",
    n_worst_politicians: int = 20,
    n_worst_polls: int = 5,
):
    """Analyzes and displays the model's prediction performance.

    This function performs several analyses on the validation set predictions:
    1. Calculates and logs the overall accuracy.
    2. Creates a confusion matrix (though it's not explicitly displayed, the data is prepared).
    3. Identifies and displays the politicians whose votes were most inaccurately predicted.
    4. Identifies and displays the polls that were most inaccurately predicted.

    Args:
        learn (TabularLearner): The trained fastai learner.
        df_all_votes (pl.DataFrame): The complete DataFrame of all votes.
        df_mandates (pl.DataFrame): The DataFrame containing mandate and politician information.
        df_polls (pl.DataFrame): The DataFrame containing poll information.
        splits (tuple[list[int], list[int]]): The train/validation splits of indices.
        y_col (str, optional): The name of the target variable column. Defaults to "vote".
        n_worst_politicians (int, optional): The number of most inaccurately predicted politicians to display. Defaults to 20.
        n_worst_polls (int, optional): The number of most inaccurately predicted polls to display. Defaults to 5.
    """

    from IPython.display import display

    y_pred, _ = learn.get_preds()
    make_numpy = lambda x: x.detach().numpy()
    y_pred = make_numpy(y_pred)

    make_pred_readable = lambda x: [learn.dls.vocab[i] for i in x.argmax(axis=1)]

    pred_col = f"{y_col}_pred"
    df_all_votes = df_all_votes.with_row_index(name="index")
    y_pred_reabale = make_pred_readable(y_pred)

    df_valid = df_all_votes.filter(pl.col("index").is_in(pl.lit(list(splits[1]))))

    df_valid = df_valid.with_columns(**{pred_col: pl.Series(y_pred_reabale)})

    M = df_valid.select([y_col, pred_col]).pivot(
        index=y_col,
        on=pred_col,
        aggregate_function="len",
    )
    M = M.fill_null(0.0)

    df_valid = df_valid.with_columns(
        **{"prediction_correct": pl.col("vote") == pl.col("vote_pred")}
    )

    acc = df_valid["prediction_correct"].sum() / len(df_valid)
    logger.info(f"Overall accuracy = {acc * 100:.2f} %")

    df_valid = df_valid.join(
        df_mandates.select(["politician", "party"]),
        left_on="politician name",
        right_on="politician",
    ).join(df_polls.select(["poll_id", "poll_title"]), on="poll_id")

    print(f"\n{n_worst_politicians} most inaccurately predicted politicians:")
    tmp = (
        df_valid.group_by(["politician name", "party"])
        .agg(**{"prediction_correct": pl.col("prediction_correct").mean()})
        .sort("prediction_correct", descending=False)
        .head(n_worst_politicians)
    )
    display(tmp)

    print(f"\n{n_worst_polls} most inaccurately predicted polls:")
    tmp = (
        df_valid.group_by(["poll_id", "poll_title"])
        .agg(**{"prediction_correct": pl.col("prediction_correct").mean()})
        .sort("prediction_correct", descending=False)
        .head(n_worst_polls)
    )
    display(tmp)


def get_embeddings(
    learn: TabularLearner,
    transform_func=lambda x: decomposition.PCA(n_components=2).fit_transform(
        x.detach().numpy()
    ),
) -> dict[str, pd.DataFrame]:
    """Extracts and optionally transforms embeddings from a trained fastai TabularLearner.

    This function iterates through the embedding layers of the model for each categorical
    variable, extracts the embedding weights, and applies a transformation function
    (by default, PCA to reduce to 2 dimensions).

    Args:
        learn (TabularLearner): The trained fastai learner containing the model with embeddings.
        transform_func (callable, optional): A function to apply to the extracted embedding tensors.
                                            Defaults to a lambda function that performs PCA to 2 components.
                                            If set to None or another non-callable, no transformation is applied.

    Returns:
        dict[str, pd.DataFrame]: A dictionary where keys are the names of the categorical variables
                                 and values are pandas DataFrames containing the (transformed) embeddings.
                                 Each DataFrame includes the original category labels.
    """

    embeddings = {}

    for i, name in enumerate(learn.dls.classes):
        t = torch.tensor(range(len(learn.dls.classes[name])))
        emb = learn.model.embeds[i](t)  # type: ignore
        if callable(transform_func):
            emb = transform_func(emb)
        embeddings[name] = pd.DataFrame(
            emb,  # type: ignore
            columns=[
                f"{name}__emb_component_{i}"
                for i in range(emb.shape[1])  # type: ignore
            ],
        ).assign(**{name: learn.dls.classes[name]})

    return embeddings


def get_poll_proponents(
    df_all_votes: pl.DataFrame, df_mandates: pl.DataFrame
) -> pl.DataFrame:
    """Identifies the political party that most strongly supported each poll.

    "Strongest support" is defined as the party with the highest percentage of "yes"
    votes among its members for a given poll.

    Args:
        df_all_votes (pl.DataFrame): DataFrame containing all vote records, including
                                     'poll_id', 'vote', and 'politician name'.
        df_mandates (pl.DataFrame): DataFrame with mandate information, linking
                                    'politician' names to their 'party'.

    Returns:
        pl.DataFrame: A DataFrame with one row per 'poll_id', indicating the
                      'strongest proponent' party for that poll.
    """

    votes_slim = df_all_votes.select(["poll_id", "vote", "politician name"])
    politician_votes = votes_slim.join(
        df_mandates.select(["politician", "party"]),
        left_on="politician name",
        right_on="politician",
    )

    poll_agreement = (
        politician_votes.group_by(["poll_id", "party"])
        .agg(
            yesses=pl.col("vote").eq(pl.lit("yes")).sum(),
            total=pl.col("vote").count(),
        )
        .with_columns(**{"yes %": pl.col("yesses") / pl.col("total") * 100.0})
    )

    proponents = (
        poll_agreement.sort("yes %", descending=False)
        .group_by(["poll_id"], maintain_order=True)
        .last()
        .rename({"party": "strongest proponent"})
    )

    return proponents


def plot_poll_embeddings(
    df_all_votes: pl.DataFrame,
    df_polls: pl.DataFrame,
    embeddings: dict[str, pl.DataFrame],
    df_mandates: pl.DataFrame,
    colors: scale_color_manual,
    col: str = "poll_id",
) -> ggplot:
    """Visualizes poll embeddings in a 2D scatter plot.

    This function takes poll embeddings (typically reduced to 2 dimensions via PCA),
    joins them with poll metadata (like title and strongest proponent party), and
    creates a scatter plot where each point is a poll, colored by the party that
    most strongly supported it.

    Args:
        df_all_votes (pl.DataFrame): DataFrame containing all vote data to determine proponents.
        df_polls (pl.DataFrame): DataFrame with poll metadata, including 'poll_title'.
        embeddings (dict[str, pl.DataFrame]): A dictionary of embedding DataFrames, where the key
                                              is the name of the embedded feature (e.g., 'poll_id').
        df_mandates (pl.DataFrame): DataFrame with mandate data to link politicians to parties.
        colors (scale_color_manual): A plotnine color scale for coloring the points by party.
        col (str, optional): The name of the column containing the poll identifiers.
                             Defaults to "poll_id".

    Returns:
        ggplot: A plotnine ggplot object representing the scatter plot of poll embeddings.
    """
    tmp = (
        df_all_votes.unique(subset=col)
        .join(
            df_polls.select([col, "poll_title"]),
            on=col,
        )
        .join(embeddings[col], on=col)
    )

    proponents = get_poll_proponents(df_all_votes, df_mandates)
    tmp = tmp.join(proponents.select([col, "strongest proponent"]), on=col)

    x = f"{col}__emb_component_0"
    y = f"{col}__emb_component_1"

    return (
        ggplot(
            tmp,
            aes(x, y, color="strongest proponent"),
        )
        + geom_point()
        + labs(
            title="Poll embeddings",
            x="PCA dim #0",
            y="PCA dim #1",
        )
        + colors
    )


def plot_politician_embeddings(
    df_all_votes: pl.DataFrame,
    df_mandates: pl.DataFrame,
    embeddings: dict[str, pl.DataFrame],
    colors: scale_color_manual,
    col: str = "politician name",
    palette: dict[str, str] | None = None,
) -> ggplot:
    """Visualizes politician embeddings in a 2D scatter plot.

    This function takes politician embeddings (typically reduced to 2 dimensions),
    joins them with mandate data to get party affiliation, and creates a scatter
    plot where each point is a politician, colored by their party.

    Args:
        df_all_votes (pl.DataFrame): DataFrame containing all vote data, used to identify
                                     unique mandates.
        df_mandates (pl.DataFrame): DataFrame linking mandates to parties and politicians.
        embeddings (dict[str, pl.DataFrame]): A dictionary of embedding DataFrames, with keys
                                              like 'politician name'.
        colors (scale_color_manual): A plotnine color scale for the plot.
        col (str, optional): The column name for the politician identifier in the embeddings.
                             Defaults to "politician name".
        palette (dict[str, str] | None, optional): A color palette dictionary to use.
                                                   Defaults to a predefined PALETTE.

    Returns:
        ggplot: A plotnine ggplot object showing the politician embeddings.
    """
    tmp = (
        df_all_votes.unique(subset="mandate_id")
        .join(
            df_mandates.select(["mandate_id", "party"]),
            on="mandate_id",
        )
        .join(embeddings[col], on=col)
    )

    palette = PALETTE if palette is None else palette

    x = f"{col}__emb_component_0"
    y = f"{col}__emb_component_1"

    return (
        ggplot(
            tmp,
            aes(x, y, color="party"),
        )
        + geom_point()
        + labs(
            title="Mandate embeddings",
            x="PCA dim #0",
            y="PCA dim #1",
        )
        + colors
    )
