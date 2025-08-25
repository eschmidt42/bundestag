import logging

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import torch
from fastai.tabular.all import TabularLearner
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
    df: pd.DataFrame,
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

    mask1 = df[poll_col].isin(polls1)
    ix0 = df.loc[~mask1].index.values
    ix1 = df.loc[mask1].index.values

    if shuffle:
        rng.shuffle(ix0)
        rng.shuffle(ix1)

    return (ix0.tolist(), ix1.tolist())


def plot_predictions(
    learn: TabularLearner,
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    df_polls: pd.DataFrame,
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
    logger.info(f"Overall accuracy = {acc * 100:.2f} %")

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
    df_all_votes: pd.DataFrame, df_mandates: pd.DataFrame
) -> pd.DataFrame:
    "Computes which party most strongly endorsed (% yes votes of party) a poll"

    votes_slim = df_all_votes[["poll_id", "vote", "politician name"]]
    politician_votes = votes_slim.join(
        df_mandates[["politician", "party"]].set_index("politician"),
        on="politician name",
    )

    poll_agreement = (
        politician_votes.groupby(["poll_id", "party"])
        .agg(
            yesses=pd.NamedAgg("vote", lambda x: (x == "yes").sum()),
            total=pd.NamedAgg("vote", "count"),
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
    df_mandates: pd.DataFrame,
    col: str = "poll_id",
    palette: dict[str, str] | None = None,
) -> go.Figure:
    tmp = (
        df_all_votes.drop_duplicates(subset=col)
        .join(
            df_polls[[col, "poll_title"]].set_index(col),
            on=col,
        )
        .join(embeddings[col].set_index(col), on=col)
    )

    proponents = get_poll_proponents(df_all_votes, df_mandates)
    tmp = tmp.join(proponents[["strongest proponent"]], on=col)

    palette = PALETTE if palette is None else palette

    return px.scatter(
        data_frame=tmp,
        x=f"{col}__emb_component_0",
        y=f"{col}__emb_component_1",
        title="Poll embeddings",
        hover_data=["poll_title"],
        color="strongest proponent",
        color_discrete_map=palette,
    )


def plot_politician_embeddings(
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    embeddings: dict[str, pd.DataFrame],
    col: str = "politician name",
    palette: dict[str, str] | None = None,
) -> go.Figure:
    tmp = (
        df_all_votes.drop_duplicates(subset="mandate_id")
        .join(
            df_mandates[["mandate_id", "party"]].set_index("mandate_id"),
            on="mandate_id",
        )
        .join(embeddings[col].set_index(col), on=col)
    )

    palette = PALETTE if palette is None else palette

    return px.scatter(
        data_frame=tmp,
        x=f"{col}__emb_component_0",
        y=f"{col}__emb_component_1",
        title="Mandate embeddings",
        color="party",
        hover_data=["politician name"],
        color_discrete_map=palette,
    )
