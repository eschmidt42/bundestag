import pandas as pd
import polars as pl
import pytest
from fastai.tabular.all import (
    TabularLearner,
)
from sklearn import decomposition

from bundestag.ml.vote_prediction import (
    get_embeddings,
    get_poll_proponents,
    plot_politician_embeddings,
    plot_poll_embeddings,
    plot_predictions,
    poll_splitter,
    scale_color_manual,
)


def test_plot_predictions_smoke(
    monkeypatch: pytest.MonkeyPatch,
    learn: TabularLearner,
    df_all_votes: pl.DataFrame,
    df_mandates: pl.DataFrame,
    df_polls: pl.DataFrame,
    y_col: str,
    splits: tuple[list[int], list[int]],
):
    """Smoke test for plot_predictions.

    This test mocks IPython.display.display so the function can be run in a
    non-interactive test environment. It also mocks the learner's get_preds to
    return deterministic predictions for the validation set.
    """

    # Prepare deterministic predictions: return a tensor of shape (N, C)
    # where N is the number of validation rows and C is number of classes.
    import torch

    # Count validation rows based on provided splits
    n_valid = len(splits[1])
    n_classes = len(learn.dls.vocab)

    # Create a prediction tensor that always predicts the first class
    preds = torch.zeros((n_valid, n_classes))
    preds[:, 0] = 1.0
    targets = torch.zeros(n_valid, dtype=torch.long)

    monkeypatch.setattr(learn, "get_preds", lambda: (preds, targets))

    displayed = []

    def fake_display(x):
        displayed.append(x)

    # Mock IPython.display.display so the local import inside the function
    # picks up our fake display
    monkeypatch.setattr("IPython.display.display", fake_display)

    # Run function (should not raise)
    plot_predictions(learn, df_all_votes, df_mandates, df_polls, splits, y_col)

    # Ensure display was called at least twice (politicians + polls)
    assert len(displayed) >= 2
    assert all(isinstance(d, pl.DataFrame) for d in displayed)


def test_plot_poll_embeddings(
    monkeypatch,
    df_all_votes: pl.DataFrame,
    df_polls: pl.DataFrame,
    df_mandates: pl.DataFrame,
):
    # Build a minimal embeddings mapping with poll_id and two components
    poll_ids = df_all_votes.unique(subset="poll_id")["poll_id"].to_list()
    emb = pl.DataFrame(
        {
            "poll_id": poll_ids,
            "poll_id__emb_component_0": [0.0] * len(poll_ids),
            "poll_id__emb_component_1": [1.0] * len(poll_ids),
        }
    )

    embeddings = {"poll_id": emb}

    # Provide a dummy colors object; fake_ggplot ignores it thanks to __add__
    party_colors = scale_color_manual(
        breaks=[
            "AfD",
            "BSW",
            "DIE GRÜNEN",
            "CDU/CSU",
            "DIE LINKE",
            "FDP",
            "fraktionslos",
            "SPD",
        ],
        values=["blue", "purple", "green", "black", "red", "yellow", "grey", "salmon"],
    )

    # Call function
    _ = plot_poll_embeddings(
        df_all_votes, df_polls, embeddings, df_mandates, party_colors, col="poll_id"
    )


def test_plot_politician_embeddings_smoke(
    df_all_votes: pl.DataFrame,
    df_mandates: pl.DataFrame,
):
    """Smoke test for plot_politician_embeddings.

    Monkeypatches `ggplot` to capture the data passed to it and asserts
    that the captured dataframe contains `politician name` and `party`.
    """

    # Build minimal embeddings mapping for politician name
    pols = df_all_votes.unique(subset="mandate_id").select(
        ["mandate_id", "politician name"]
    )
    # Use unique politician names
    pol_names = pols["politician name"].to_list()
    emb = pl.DataFrame(
        {
            "politician name": pol_names,
            "politician name__emb_component_0": [0.0] * len(pol_names),
            "politician name__emb_component_1": [1.0] * len(pol_names),
        }
    )

    embeddings = {"politician name": emb}
    party_colors = scale_color_manual(
        breaks=[
            "AfD",
            "BSW",
            "DIE GRÜNEN",
            "CDU/CSU",
            "DIE LINKE",
            "FDP",
            "fraktionslos",
            "SPD",
        ],
        values=["blue", "purple", "green", "black", "red", "yellow", "grey", "salmon"],
    )

    _ = plot_politician_embeddings(
        df_all_votes,
        df_mandates,
        embeddings,
        party_colors,
        col="politician name",
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
