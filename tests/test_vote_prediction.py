import pandas as pd
import polars as pl
import pytest
from fastai.tabular.all import (
    TabularLearner,
)
from sklearn import decomposition

from bundestag.vote_prediction import (
    get_embeddings,
    get_poll_proponents,
    plot_predictions,
    poll_splitter,
)


@pytest.mark.parametrize("seed,shuffle", [(None, True), (42, False)])
def test_poll_splitter(seed: int, shuffle: bool):
    c = "poll"
    df = pl.DataFrame(
        {
            c: list(range(10)),
        }
    )

    # line to test
    splits0 = poll_splitter(df, poll_col=c, seed=seed, shuffle=shuffle)
    splits1 = poll_splitter(df, poll_col=c, seed=seed, shuffle=shuffle)

    assert isinstance(splits0, tuple)
    assert len(splits0) == 2

    if not shuffle and seed == 42:
        assert splits0 == splits1
    else:
        assert splits0 != splits1


@pytest.mark.slow
@pytest.mark.parametrize("transform", [None, "pca"])
def test_get_embeddings(transform: str | None, learn: TabularLearner):
    if transform == "pca":
        transform_func = lambda x: decomposition.PCA(n_components=2).fit_transform(
            x.detach().numpy()
        )
    else:
        transform_func = None

    # Move model to CPU to avoid MPS device issues
    learn.model.cpu()  # type: ignore

    emb = get_embeddings(learn, transform_func=transform_func)

    assert isinstance(emb, dict)
    assert all([isinstance(m, pd.DataFrame) for m in emb.values()])
    if transform == "pca":
        assert all([m.shape[1] == 2 + 1 for m in emb.values()])
    else:
        assert all([m.shape[1] > 2 + 1 for m in emb.values()])
    assert all([k in emb for k in learn.dls.classes])


def test_get_poll_proponents(df_all_votes: pl.DataFrame, df_mandates: pl.DataFrame):
    proponents = get_poll_proponents(df_all_votes, df_mandates)

    exp_cols = ["strongest proponent", "yesses", "total", "yes %", "poll_id"]
    assert all([c in proponents.columns for c in exp_cols])
    assert proponents["poll_id"].n_unique() == len(proponents)
    assert proponents["yes %"].max() <= 100
    assert proponents["yes %"].min() >= 0
    assert proponents["total"].min() >= 0
    assert proponents["yesses"].min() >= 0
    assert (proponents["yesses"] <= proponents["total"]).all()


@pytest.mark.skip("skipping plot")
def test_plot_predictions(
    learn: TabularLearner,
    df_all_votes: pl.DataFrame,
    df_mandates: pl.DataFrame,
    df_polls: pl.DataFrame,
    y_col: str,
    splits: tuple[list[int], list[int]],
):
    plot_predictions(learn, df_all_votes, df_mandates, df_polls, splits, y_col)
