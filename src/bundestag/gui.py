import datetime

import ipywidgets as widgets
import matplotlib.pyplot as plt
import polars as pl

from bundestag import similarity as sim


class GUI:
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
        self.df = df
        self.mdbs = df["Bezeichnung"].unique()
        self.parties = df["Fraktion/Gruppe"].unique()
        self.party_votes = sim.get_votes_by_party(df)
        self.init_widgets()

    def init_widgets(self):
        raise NotImplementedError(
            "Not implemented in GUI, needs to be specified by child class"
        )

    def render(self):
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
        if start_date is None or end_date is None:
            return self.df, self.party_votes

        df = self.df.filter(pl.col("date").is_between(start_date, end_date))
        # mask = (self.df["date"] >= str(start_date)) & (self.df["date"] <= str(end_date))
        # df = self.df.loc[mask]
        party_votes = self.party_votes.filter(
            pl.col("date").is_between(start_date, end_date)
        )
        # date_range = pl.date_range(start_date, end_date)
        # mask = self.party_votes["date"].isin(date_range)
        # party_votes = self.party_votes.loc[mask]
        return df, party_votes


class MdBGUI(GUI):
    def init_widgets(self):
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
    def init_widgets(self):
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
