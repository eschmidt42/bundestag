import pandas as pd
from loguru import logger
import sys

import numpy as np
import plotly.express as px
from fastcore.all import *
import torch

from sklearn import decomposition

logger.remove()
logger.add(sys.stderr, level="INFO")
# default level for this module should be INFO


def poll_splitter(
    df: pd.DataFrame,
    poll_col: str = "poll_id",
    valid_pct: float = 0.2,
    shuffle: bool = True,
):
    polls = df[poll_col].unique()
    n = len(polls)
    valid_polls = np.random.choice(polls, size=int(n * valid_pct))
    logger.debug(
        f"Splitting votes by polls (num train = {n-len(valid_polls)}, num valid = {len(valid_polls)})"
    )
    valid_mask = df[poll_col].isin(valid_polls)
    ix_train = df.loc[~valid_mask].index.values
    ix_valid = df.loc[valid_mask].index.values
    if shuffle:
        np.random.shuffle(ix_train)
        np.random.shuffle(ix_valid)
    return (L(ix_train.tolist()), L(ix_valid.tolist()))


def test_poll_split(split):
    assert isinstance(split, tuple)
    assert len(split) == 2


def plot_predictions(
    learn,
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    df_polls: pd.DataFrame,
    splits,
    y_col: str = "vote",
    n_worst_politicians: int = 20,
    n_worst_polls: int = 5,
):
    "Plot absolute and relative confusion matrix as well as the accuracy."

    y_pred, y_targ = learn.get_preds()
    make_numpy = lambda x: x.detach().numpy()
    y_pred = make_numpy(y_pred)

    make_pred_readable = lambda x: [learn.dls.vocab[i] for i in x.argmax(axis=1)]

    pred_col = f"{y_col}_pred"
    df_valid = df_all_votes.iloc[splits[1], :].assign(
        **{pred_col: make_pred_readable(y_pred)}
    )

    M = df_valid[[y_col, pred_col]].pivot_table(
        index=y_col, columns=pred_col, aggfunc=len, fill_value=0
    )

    display(
        M.style.background_gradient(axis=1),
        M.assign(total=M.sum(axis=1))
        .pipe(lambda x: (x.T / x["total"]).T)
        .drop(columns=["total"])
        .style.background_gradient(axis=1),
    )
    df_valid["prediction_correct"] = df_valid["vote"] == df_valid["vote_pred"]

    acc = df_valid["prediction_correct"].sum() / len(df_valid)
    logger.info(f"Overall accuracy = {acc*100:.2f} %")

    df_valid = df_valid.join(
        df_mandates[["politician", "party"]].set_index("politician"),
        on="politician name",
    ).join(df_polls[["poll_id", "poll_title"]].set_index("poll_id"), on="poll_id")

    print(f"\n{n_worst_politicians} most inaccurately predicted politicians:")
    tmp = (
        df_valid.groupby(["politician name", "party"])["prediction_correct"]
        .mean()
        .sort_values(ascending=True)
        .head(n_worst_politicians)
        .reset_index()
    )
    display(tmp)

    print(f"\n{n_worst_polls} most inaccurately predicted polls:")
    tmp = (
        df_valid.groupby(["poll_id", "poll_title"])["prediction_correct"]
        .mean()
        .sort_values(ascending=True)
        .head(n_worst_polls)
        .reset_index()
    )
    display(tmp)


def get_embeddings(
    learn,
    transform_func=lambda x: decomposition.PCA(n_components=2).fit_transform(
        x.detach().numpy()
    ),
):
    """Collects embeddings from tabular_learner.model and returns them with optional transformation
    via `transform_func` (e.g. sklearn.decomposition.PCA)"""
    embeddings = {}
    for i, name in enumerate(learn.dls.classes):
        emb = learn.model.embeds[i](torch.tensor(range(len(learn.dls.classes[name]))))
        if callable(transform_func):
            emb = transform_func(emb)
        embeddings[name] = pd.DataFrame(
            emb, columns=[f"{name}__emb_component_{i}" for i in range(emb.shape[1])]
        ).assign(**{name: learn.dls.classes[name]})

    return embeddings


def test_embeddings(emb: dict):
    assert isinstance(emb, dict)
    assert all([isinstance(m, pd.DataFrame) for m in emb.values()])


def test_poll_proponents(proponents: pd.DataFrame):
    assert proponents.index.nunique() == len(proponents)


def get_poll_proponents(df_all_votes: pd.DataFrame, df_mandates: pd.DataFrame):
    "Computes which party most strongly endorsed (% yes votes of party) a poll"

    poll_agreement = (
        df_all_votes[["poll_id", "vote", "politician name"]]
        .join(
            df_mandates[["politician", "party"]].set_index("politician"),
            on="politician name",
        )
        .groupby(["poll_id", "party"])
        .agg(
            **{
                "yesses": pd.NamedAgg("vote", lambda x: (x == "yes").sum()),
                "total": pd.NamedAgg("vote", "count"),
            }
        )
        .assign(**{"yes %": lambda x: x["yesses"] / x["total"] * 100})
    )

    proponents = (
        poll_agreement.reset_index()
        .sort_values("yes %")
        .groupby(["poll_id"])
        .last()
        .rename(columns={"party": "strongest proponent"})
    )

    return proponents


def plot_poll_embeddings(
    df_all_votes: pd.DataFrame,
    df_polls: pd.DataFrame,
    embeddings: dict,
    df_mandates: pd.DataFrame = None,
):

    col = "poll_id"

    tmp = (
        df_all_votes.drop_duplicates(subset="poll_id")
        .join(df_polls[["poll_id", "poll_title"]].set_index("poll_id"), on="poll_id")
        .join(embeddings[col].set_index(col), on=col)
    )

    if df_mandates is not None:
        proponents = get_poll_proponents(df_all_votes, df_mandates)
        tmp = tmp.join(proponents[["strongest proponent"]], on=col)
    return px.scatter(
        data_frame=tmp,
        x=f"{col}__emb_component_0",
        y=f"{col}__emb_component_1",
        title="Poll embeddings",
        hover_data=["poll_title"],
        color="strongest proponent",
    )


def plot_politician_embeddings(
    df_all_votes: pd.DataFrame, df_mandates: pd.DataFrame, embeddings: dict
):

    col = "politician name"

    tmp = (
        df_all_votes.drop_duplicates(subset="mandate_id")
        .join(
            df_mandates[["mandate_id", "party"]].set_index("mandate_id"),
            on="mandate_id",
        )
        .join(embeddings[col].set_index(col), on=col)
    )

    return px.scatter(
        data_frame=tmp,
        x=f"{col}__emb_component_0",
        y=f"{col}__emb_component_1",
        title="Mandate embeddings",
        color="party",
        hover_data=["politician name"],
    )
