from bundestag import similarity as sim
import ipywidgets as widgets
import pandas as pd
import matplotlib.pyplot as plt


class GUI:
    def __init__(self, df: pd.DataFrame):
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

    def filter_dfs(self, start_date, end_date):
        if start_date is None or end_date is None:
            return self.df, self.party_votes

        mask = (self.df["date"] >= str(start_date)) & (self.df["date"] <= str(end_date))
        df = self.df.loc[mask]

        date_range = pd.date_range(start_date, end_date)
        mask = self.party_votes["date"].isin(date_range)
        party_votes = self.party_votes.loc[mask]
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

        mdb_vs_parties = sim.align_mdb_with_parties(
            mdb_votes, party_votes_pivoted
        ).pipe(sim.compute_similarity, lsuffix="mdb", rsuffix="party")

        self.display_widget.clear_output()
        with self.display_widget:
            sim.plot(
                mdb_vs_parties,
                title_overall=f"Overall similarity of {mdb} with all parties",
                title_over_time=f"{mdb} vs time",
                party_col="Fraktion/Gruppe",
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

        df, party_votes = self.filter_dfs(start_date, end_date)
        party_votes_pivoted = sim.pivot_party_votes_df(party_votes)

        partyA_vs_rest = sim.align_party_with_all_parties(
            party_votes_pivoted, party
        ).pipe(sim.compute_similarity, lsuffix="a", rsuffix="b")

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
