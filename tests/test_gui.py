import datetime

import polars as pl
import pytest

from bundestag.gui import MdBGUI, PartyGUI


@pytest.fixture(autouse=True)
def use_matplotlib_agg():
    """Force matplotlib to use the Agg backend in tests to avoid GUI popups."""
    import matplotlib

    matplotlib.use("Agg")

    # silence the non-interactive FigureCanvasAgg show warning from plt.show()
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="FigureCanvasAgg is non-interactive, and thus cannot be shown",
        category=UserWarning,
    )

    yield


def test_init_and_filter():
    # Build synthetic sheet-like data so get_votes_by_party can run normally.
    # We create two voters in the same poll (same date/title) with complementary votes.
    df = pl.DataFrame(
        {
            "Bezeichnung": ["Alice", "Bob"],
            "Fraktion/Gruppe": ["A", "A"],
            "date": [datetime.date(2020, 1, 1), datetime.date(2020, 1, 1)],
            "title": ["Poll 1", "Poll 1"],
            # single 'vote' column as produced by get_squished_dataframe/create_vote_column
            "vote": ["ja", "nein"],
        }
    )

    # No mocking here: allow get_votes_by_party to compute fractions from the synthetic data
    gui = MdBGUI(df)

    # name widget must contain MdB names
    assert "Alice" in tuple(gui.name_widget.options)

    # filter_dfs should reduce rows when providing a tight date range
    start = datetime.date(2020, 1, 1)
    end = datetime.date(2020, 1, 1)
    df_filtered, party_filtered = gui.filter_dfs(start, end)
    # two voters in the same poll -> original df filtered has 2 rows
    assert df_filtered.height == 2
    # get_votes_by_party will produce one row per vote outcome (ja/nein) for the party
    assert party_filtered.height == 2


def test_mdb_on_click_triggers_plot(monkeypatch):
    # Ensure the plotting palette contains our synthetic party keys
    monkeypatch.setattr(
        "bundestag.ml.similarity.PALETTE", {"A": "grey", "B": "lightgrey"}
    )
    # Build synthetic data that exercises the real similarity pipeline.
    # Two members from party A vote differently in the same poll, plus one from B.
    df = pl.DataFrame(
        {
            "Bezeichnung": ["Alice", "Bob", "Carol"],
            "Fraktion/Gruppe": ["A", "A", "B"],
            "date": [datetime.date(2020, 1, 1)] * 3,
            "title": ["Poll X"] * 3,
            "vote": ["ja", "nein", "ja"],
        }
    )

    # Track calls
    # Allow real plotting to run headlessly under the Agg backend

    gui = MdBGUI(df)
    # select MdB and date range
    gui.name_widget.value = "Alice"
    gui.start_widget.value = datetime.date(2020, 1, 1)
    gui.end_widget.value = datetime.date(2020, 1, 1)

    # call the click handler
    gui.on_click(None)

    assert "Selected: MdB = Alice" in gui.selection_widget.value


def test_party_on_click_triggers_plot(monkeypatch):
    # Ensure the plotting palette contains our synthetic party keys
    monkeypatch.setattr(
        "bundestag.ml.similarity.PALETTE", {"A": "grey", "B": "lightgrey"}
    )
    # Build synthetic data with two parties and one poll so the party pipeline can run.
    df = pl.DataFrame(
        {
            "Bezeichnung": ["Alice", "Bob", "Eve"],
            "Fraktion/Gruppe": ["A", "A", "B"],
            "date": [datetime.date(2020, 1, 1)] * 3,
            "title": ["Poll Y"] * 3,
            "vote": ["ja", "ja", "nein"],
        }
    )

    # Allow real plotting to run headlessly under the Agg backend

    gui = PartyGUI(df)
    gui.name_widget.value = "A"
    gui.start_widget.value = datetime.date(2020, 1, 1)
    gui.end_widget.value = datetime.date(2020, 1, 1)

    gui.on_click(None)

    assert "Selected: Party = A" in gui.selection_widget.value
