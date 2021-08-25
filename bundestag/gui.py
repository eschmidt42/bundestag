from bundestag import similarity
import ipywidgets as widgets
import pandas as pd

# pd.options.plotting.backend = "plotly"

class GUI:
    def __init__(self, df:pd.DataFrame):
        self.df = df
        self.party_votes, self.df_plot = similarity.get_votes_by_party(df)
        self.mdbs = df['Bezeichnung'].unique()
        self.parties = df['Fraktion/Gruppe'].unique()
        self.init_widgets()
    def render(self):
        return widgets.VBox([
            self.mdb_widget, 
            self.party_widget,
            self.start_widget,
            self.end_widget,
            self.submit_widget, 
            self.selection_widget,
            self.display_widget,
        ])
    def on_click(self, change):
        mdb = self.mdb_widget.value
        party = self.party_widget.value
        assert mdb is not None, 'Please choose an MdB'
        assert party is not None, 'Please choose a Party'
        start_date = self.start_widget.value
        end_date = self.end_widget.value
        self.selection_widget.value = f'Selected: MdB = {mdb}, Party = {party}, date range = {start_date} - {end_date}'
        
        df, party_votes = self.filter_dfs(start_date, end_date)
        
        mdb_votes = similarity.prepare_votes_of_mdb(df, mdb)
        similarity_mdb_party = (similarity.align_mdb_with_parties(mdb_votes, party_votes)
                                .pipe(similarity.compute_similarity, lsuffix='mdb', rsuffix='party'))
        
        similarity_party_party = (similarity.align_party_with_all_parties(party_votes, party)
                                  .pipe(similarity.compute_similarity, lsuffix='a', rsuffix='b'))
        
        self.display_widget.clear_output()
        with self.display_widget:
            similarity.plot_similarity_over_time(similarity_mdb_party, 
                                                 'Fraktion/Gruppe',
                                                 title=f'{mdb} vs time').show()
            similarity.plot_similarity_over_time(similarity_party_party, 
                                                 'Fraktion/Gruppe_b',
                                                 title=f'{party} vs time').show()
        
    def init_widgets(self):
        # prints out the current selection of MdB, party and timespan
        self.selection_widget = widgets.Label()
        
        # widget to render figures in
        self.display_widget = widgets.Output()
        
        # submit button
        self.submit_widget = widgets.Button(description='Submit')
        self.submit_widget.on_click(self.on_click)
        
        # widgets to set MdB, party, timespan
        self.mdb_widget = widgets.Combobox(
            placeholder='Choose an MdB',
            options=tuple(self.mdbs),
            description='MdB:',
            ensure_option=True,
        )
        self.party_widget = widgets.Combobox(
            placeholder='Choose a Party',
            options=tuple(self.parties),
            description='Party:',
            ensure_option=True,
        )
        self.start_widget = widgets.DatePicker(description='Start date')
        self.end_widget = widgets.DatePicker(description='End date')
            
    def filter_dfs(self, start_date, end_date):
        if (start_date is None or end_date is None):
            return self.df, self.party_votes

        mask = (self.df['date']>=str(start_date)) & (self.df['date']<=str(end_date))
        df = self.df.loc[mask]

        date_range = pd.date_range(start_date, end_date)
        mask = self.party_votes.index.get_level_values('date').isin(date_range)
        party_votes = self.party_votes.loc[mask]
        return df, party_votes