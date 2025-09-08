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
    "Split the polls into train and test set."

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
        **{"is validation set": pl.col(poll_col).is_in(pl.lit(polls1))}
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
    "Plot absolute and relative confusion matrix as well as the accuracy."

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

    # display(

    #     M.with_columns(total=M.sum_horizontal())
    #     .pipe(lambda x: (x.T / x["total"]).T)
    #     .drop(columns=["total"])
    #     .style.background_gradient(axis=1),
    # )
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
    """Collects embeddings from tabular_learner.model and returns them with optional transformation
    via `transform_func` (e.g. sklearn.decomposition.PCA)"""
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
    "Computes which party most strongly endorsed (% yes votes of party) a poll"

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
