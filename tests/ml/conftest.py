from pathlib import Path

import pandas as pd
import polars as pl
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

from bundestag.ml.vote_prediction import get_embeddings, poll_splitter


@pytest.fixture()
def embeddings(learn: TabularLearner) -> dict[str, pd.DataFrame]:
    transform_func = lambda x: decomposition.PCA(n_components=2).fit_transform(
        x.detach().numpy()
    )

    return get_embeddings(learn, transform_func=transform_func)


@pytest.fixture()
def splits(df_all_votes: pl.DataFrame) -> tuple[list[int], list[int]]:
    return poll_splitter(df_all_votes, valid_pct=0.2)


@pytest.fixture()
def learn(data_loaders: DataLoaders) -> TabularLearner:
    lr = 0.002
    learn = tabular_learner(data_loaders)  # type: ignore
    learn.fit_one_cycle(5, lr)
    return learn


@pytest.fixture()
def data_loaders(tabular_object: TabularPandas) -> DataLoaders:
    return tabular_object.dataloaders(bs=512)


@pytest.fixture()
def tabular_object(
    df_all_votes: pl.DataFrame,
    splits: tuple[list[int], list[int]],
    y_col: str,
) -> TabularPandas:
    return TabularPandas(
        df_all_votes.to_pandas(),
        cat_names=["politician name", "poll_id"],
        y_names=[y_col],
        procs=[Categorify],
        y_block=CategoryBlock,
        splits=splits,
    )


@pytest.fixture()
def y_col() -> str:
    return "vote"


@pytest.fixture()
def df_polls(base_path: Path) -> pl.DataFrame:
    file = base_path / "polls_111.parquet"
    return pl.read_parquet(file)


@pytest.fixture()
def df_mandates(base_path: Path) -> pl.DataFrame:
    file = base_path / "mandates_111.parquet"
    df_mandates = pl.read_parquet(file)
    df_mandates = df_mandates.with_columns(
        **{
            "party_original": pl.col("party"),
        }
    )

    return df_mandates


@pytest.fixture()
def df_all_votes(base_path: Path) -> pl.DataFrame:
    file = base_path / "votes_111.parquet"
    return pl.read_parquet(file)


@pytest.fixture()
def base_path() -> Path:
    return Path("tests/data_for_testing")
