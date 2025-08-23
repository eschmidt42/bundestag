import typing as T
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pytest
from fastai.tabular.all import (
    Categorify,
    CategoryBlock,
    DataLoaders,
    TabularLearner,
    TabularPandas,
    tabular_learner,
)
from sklearn import decomposition

# from bundestag import abgeordnetenwatch as aw
import bundestag.vote_prediction as vp

# TODO: find out why this test module just dies
pytest.skip("Currently broken, reason TBD.", allow_module_level=True)


@pytest.mark.parametrize("seed,shuffle", [(None, True), (42, False)])
def test_poll_splitter(seed: int, shuffle: bool):
    c = "poll"
    df = pd.DataFrame(
        {
            c: list(range(10)),
        }
    )

    # line to test
    splits0 = vp.poll_splitter(df, poll_col=c, seed=seed, shuffle=shuffle)
    splits1 = vp.poll_splitter(df, poll_col=c, seed=seed, shuffle=shuffle)

    assert isinstance(splits0, tuple)
    assert len(splits0) == 2

    if not shuffle and seed == 42:
        assert splits0 == splits1
    else:
        assert splits0 != splits1


@pytest.fixture(scope="module")
def base_path() -> Path:
    return Path("tests/data_for_testing")


@pytest.fixture(scope="module")
def df_all_votes(base_path: Path) -> Path:
    file = base_path / "votes_111.parquet"
    return pd.read_parquet(file)


@pytest.fixture(scope="module")
def df_mandates(base_path: Path) -> Path:
    file = base_path / "mandates_111.parquet"
    df_mandates = pd.read_parquet(file)
    df_mandates["party_original"] = df_mandates["party"].copy()
    df_mandates["party"] = df_mandates["party"].apply(lambda x: x[-1])
    return df_mandates


@pytest.fixture(scope="module")
def df_polls(base_path: Path) -> Path:
    file = base_path / "polls_111.parquet"
    return pd.read_parquet(file)


@pytest.fixture(scope="module")
def splits(df_all_votes: pd.DataFrame) -> Path:
    return vp.poll_splitter(df_all_votes, valid_pct=0.2)


@pytest.fixture(scope="module")
def y_col() -> str:
    return "vote"


@pytest.fixture(scope="module")
def to(
    df_all_votes: pd.DataFrame,
    splits: T.Tuple[T.List[int], T.List[int]],
    y_col: str,
) -> Path:
    return TabularPandas(
        df_all_votes,
        cat_names=["politician name", "poll_id"],
        y_names=[y_col],
        procs=[Categorify],
        y_block=CategoryBlock,
        splits=splits,
    )


@pytest.fixture(scope="module")
def dls(to: TabularPandas) -> DataLoaders:
    return to.dataloaders(bs=512)


@pytest.fixture(scope="module")
def learn(dls: DataLoaders) -> TabularLearner:
    lr = 0.002
    learn = tabular_learner(dls)
    learn.fit_one_cycle(5, lr)
    return learn


@pytest.fixture(scope="module")
def embeddings(learn: TabularLearner) -> T.Dict[str, pd.DataFrame]:
    transform_func = lambda x: decomposition.PCA(n_components=2).fit_transform(
        x.detach().numpy()
    )

    return vp.get_embeddings(learn, transform_func=transform_func)


@pytest.mark.slow
def test_plot_predictions(
    learn: TabularLearner,
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    df_polls: pd.DataFrame,
    y_col: str,
    splits: T.Tuple[T.List[int], T.List[int]],
):
    # line to test
    vp.plot_predictions(learn, df_all_votes, df_mandates, df_polls, splits, y_col)


@pytest.mark.slow
@pytest.mark.parametrize("transform", [None, "pca"])
def test_get_embeddings(transform, learn: TabularLearner):
    if transform == "pca":
        transform_func = lambda x: decomposition.PCA(n_components=2).fit_transform(
            x.detach().numpy()
        )
    else:
        transform_func = None

    # line to test
    emb = vp.get_embeddings(learn, transform_func=transform_func)

    assert isinstance(emb, dict)
    assert all([isinstance(m, pd.DataFrame) for m in emb.values()])
    if transform == "pca":
        assert all([m.shape[1] == 2 + 1 for m in emb.values()])
    else:
        assert all([m.shape[1] > 2 + 1 for m in emb.values()])
    assert all([k in emb for k in learn.dls.classes])


def test_get_poll_proponents(df_all_votes: pd.DataFrame, df_mandates: pd.DataFrame):
    # line to test
    proponents = vp.get_poll_proponents(df_all_votes, df_mandates)

    assert proponents.index.nunique() == len(proponents)
    exp_cols = ["strongest proponent", "yesses", "total", "yes %"]
    assert all([c in proponents.columns for c in exp_cols])
    assert proponents.index.name == "poll_id"
    assert proponents["yes %"].max() <= 100
    assert proponents["yes %"].min() >= 0
    assert proponents["total"].min() >= 0
    assert proponents["yesses"].min() >= 0
    assert (proponents["yesses"] <= proponents["total"]).all()


@pytest.mark.slow
def test_plot_poll_embeddings(
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    df_polls: pd.DataFrame,
    embeddings: dict,
):
    # line to test
    fig = vp.plot_poll_embeddings(df_all_votes, df_polls, embeddings, df_mandates)

    assert isinstance(fig, go.Figure)


@pytest.mark.slow
def test_plot_politician_embeddings(
    df_all_votes: pd.DataFrame,
    df_mandates: pd.DataFrame,
    df_polls: pd.DataFrame,
    embeddings: dict,
):
    # line to test
    fig = vp.plot_politician_embeddings(
        df_all_votes,
        df_mandates,
        embeddings,
    )

    assert isinstance(fig, go.Figure)
