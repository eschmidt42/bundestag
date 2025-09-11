import datetime

import ipywidgets as widgets
import matplotlib.pyplot as plt
import polars as pl

from bundestag.ml import similarity as sim


class GUI:
    """Base class for creating interactive GUIs in a Jupyter environment
    to explore voting similarity.

    This class provides the basic structure for a GUI, including widget
    initialization, rendering, and data filtering. It is meant to be
    subclassed to create specific GUIs (e.g., for analyzing an MdB or a party).

    Attributes:
        name_widget (widgets.Combobox): Widget to select an entity (e.g., MdB or party).
        start_widget (widgets.DatePicker): Widget to select the start date for filtering.
        end_widget (widgets.DatePicker): Widget to select the end date for filtering.
        submit_widget (widgets.Button): Button to trigger the analysis.
        selection_widget (widgets.Label): Label to display the current selection.
        display_widget (widgets.Output): Widget to display plots and other output.
        df (pl.DataFrame): The main DataFrame with voting data.
        mdbs (pl.Series): A Series of unique MdB names.
        parties (pl.Series): A Series of unique party names.
        party_votes (pl.DataFrame): A pre-processed DataFrame with votes aggregated by party.
    """

    name_widget: widgets.Combobox
    start_widget: widgets.DatePicker
    end_widget: widgets.DatePicker
    submit_widget: widgets.Button
    selection_widget: widgets.Label
    display_widget: widgets.Output
    df: pl.DataFrame
    mdbs: pl.Series
    parties: pl.Series
    party_votes: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        """Initializes the GUI with the voting data.

        Args:
            df (pl.DataFrame): The main DataFrame containing detailed voting records.
        """
        self.df = df
        self.mdbs = df["Bezeichnung"].unique()
        self.parties = df["Fraktion/Gruppe"].unique()
        self.party_votes = sim.get_votes_by_party(df)
        self.init_widgets()

    def init_widgets(self):
        """Initializes the widgets for the GUI.

        This method must be implemented by a subclass. It is responsible for
        creating and configuring all the ipywidgets that make up the GUI.
        """
        raise NotImplementedError(
            "Not implemented in GUI, needs to be specified by child class"
        )

    def render(self):
        """Renders the GUI widgets in a vertical box layout.

        Returns:
            widgets.VBox: A VBox container with all the GUI widgets.
        """
        return widgets.VBox(
            [
                self.name_widget,
                self.start_widget,
                self.end_widget,
                self.submit_widget,
                self.selection_widget,
                self.display_widget,
            ]
        )

    def filter_dfs(
        self, start_date: datetime.date | None, end_date: datetime.date | None
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Filters the main and party vote DataFrames based on a date range.

        Args:
            start_date (datetime.date | None): The start date for the filter.
            end_date (datetime.date | None): The end date for the filter.

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: A tuple containing the filtered
                                               main DataFrame and the filtered
                                               party votes DataFrame. If dates are
                                               None, original dfs are returned.
        """
        if start_date is None or end_date is None:
            return self.df, self.party_votes

        df = self.df.filter(pl.col("date").is_between(start_date, end_date))
        party_votes = self.party_votes.filter(
            pl.col("date").is_between(start_date, end_date)
        )
        return df, party_votes


class MdBGUI(GUI):
    """A GUI for analyzing the voting similarity of a single MdB (Member of Parliament)
    against all political parties.

    This class extends the base `GUI` to provide an interface for selecting an MdB
    and a date range, then plotting their voting similarity compared to each party.
    """

    def init_widgets(self):
        """Initializes the specific widgets for the MdB analysis GUI."""
        # prints out the current selection of MdB, party and timespan
        self.selection_widget = widgets.Label()

        # widget to render figures in
        self.display_widget = widgets.Output()

        # submit button
        self.submit_widget = widgets.Button(description="Submit")
        self.submit_widget.on_click(self.on_click)

        # widgets to set MdB, party, timespan
        self.name_widget = widgets.Combobox(
            placeholder="Choose an MdB",
            options=tuple(self.mdbs),
            description="MdB:",
            ensure_option=True,
        )

        self.start_widget = widgets.DatePicker(description="Start date")
        self.end_widget = widgets.DatePicker(description="End date")

    def on_click(self, change):
        """Callback function for the submit button click event.

        This function retrieves the selected MdB and date range, filters the data,
        computes the voting similarity between the MdB and all parties, and
        displays the resulting plots.

        Args:
            change: The event object from the button click.
        """
        mdb = self.name_widget.value

        assert mdb is not None, "Please choose an MdB"

        start_date = self.start_widget.value
        end_date = self.end_widget.value
        self.selection_widget.value = (
            f"Selected: MdB = {mdb}, date range = {start_date} - {end_date}"
        )

        df, party_votes = self.filter_dfs(start_date, end_date)
        party_votes_pivoted = sim.pivot_party_votes_df(party_votes)

        mdb_votes = sim.prepare_votes_of_mdb(df, mdb)

        mdb_vs_parties = sim.align_mdb_with_parties(mdb_votes, party_votes_pivoted)
        mdb_vs_parties = sim.compute_similarity(mdb_vs_parties, suffix="_party")

        self.display_widget.clear_output()
        with self.display_widget:
            sim.plot(
                mdb_vs_parties,
                title_overall=f"Overall similarity of {mdb} with all parties",
                title_over_time=f"{mdb} vs time",
                party_col="Fraktion/Gruppe_party",
            )
            plt.tight_layout()
            plt.show()


class PartyGUI(GUI):
    """A GUI for analyzing the voting similarity of a single political party
    against all other parties.

    This class extends the base `GUI` to provide an interface for selecting a party
    and a date range, then plotting its voting similarity compared to the other parties.
    """

    def init_widgets(self):
        """Initializes the specific widgets for the party analysis GUI."""
        # prints out the current selection of MdB, party and timespan
        self.selection_widget = widgets.Label()

        # widget to render figures in
        self.display_widget = widgets.Output()

        # submit button
        self.submit_widget = widgets.Button(description="Submit")
        self.submit_widget.on_click(self.on_click)

        # widgets to set MdB, party, timespan
        self.name_widget = widgets.Combobox(
            placeholder="Choose a Party",
            options=tuple(self.parties),
            description="Party:",
            ensure_option=True,
        )

        self.start_widget = widgets.DatePicker(description="Start date")
        self.end_widget = widgets.DatePicker(description="End date")

    def on_click(self, change):
        """Callback function for the submit button click event.

        This function retrieves the selected party and date range, filters the data,
        computes the voting similarity between the selected party and all others,
        and displays the resulting plots.

        Args:
            change: The event object from the button click.
        """
        party = self.name_widget.value

        assert party is not None, "Please choose a Party"

        start_date = self.start_widget.value
        end_date = self.end_widget.value
        self.selection_widget.value = (
            f"Selected: Party = {party}, date range = {start_date} - {end_date}"
        )

        _, party_votes = self.filter_dfs(start_date, end_date)
        party_votes_pivoted = sim.pivot_party_votes_df(party_votes)

        partyA_vs_rest = sim.align_party_with_all_parties(
            party_votes_pivoted, party
        ).pipe(sim.compute_similarity, suffix="_b")

        self.display_widget.clear_output()
        with self.display_widget:
            sim.plot(
                partyA_vs_rest,
                title_overall=f"Overall similarity of {party} with all other parties",
                title_over_time=f"{party} vs time",
                party_col="Fraktion/Gruppe_b",
            )
            plt.tight_layout()
            plt.show()
