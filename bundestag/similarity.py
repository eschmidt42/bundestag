import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from scipy import spatial
from bundestag import html_parsing as hp

def get_votes_by_party(df:pd.DataFrame):
    party_votes = (df.groupby(['Fraktion/Gruppe', 'date', 'title'])['vote']
                   .value_counts(normalize=True)
                   .to_frame('fraction')
                   .join(df.groupby(['Fraktion/Gruppe', 'date', 'title'])['vote']
                         .value_counts(normalize=False)
                         .to_frame('# votes')))
    df_plot = party_votes.reset_index()
    
    index = []
    for party, date, title in df_plot[['Fraktion/Gruppe', 'date', 'title']].drop_duplicates().values:
        index.extend([[party, date, title, vote] for vote in hp.VOTE_COLS])

    index = np.array(index)
    index = pd.MultiIndex.from_arrays(index.T, names=['Fraktion/Gruppe', 'date', 'title', 'vote'])
    df_plot = (df_plot
               .set_index(['Fraktion/Gruppe', 'date', 'title', 'vote'])
               .reindex(index)
               .fillna(0))
    
    party_votes = df_plot.pivot_table(index=['Fraktion/Gruppe', 'date', 'title'],
                                  columns='vote', values='fraction')
    return party_votes, df_plot.reset_index()

def prepare_votes_of_mdb(df:pd.DataFrame, mdb:str):
    assert mdb in df['Bezeichnung'].unique()
    mask = df['Bezeichnung'] == mdb

    mdb_votes = df.loc[mask, ['date', 'title', 'vote']]
    mdb_votes['vote'] = mdb_votes['vote'].astype('category')
    mdb_votes['vote'] = mdb_votes['vote'].cat.set_categories(hp.VOTE_COLS) # inplace=True
    # ['ja', 'nein', 'nichtabgegeben', 'Enthaltung']

    mdb_votes = pd.get_dummies(mdb_votes, columns=['vote'], prefix='', prefix_sep='')
    return mdb_votes    

def align_mdb_with_parties(mdb_votes:pd.DataFrame, party_votes:pd.DataFrame):
    return mdb_votes.join(party_votes.reset_index('Fraktion/Gruppe'), on=['date', 'title'], lsuffix='_mdb', rsuffix='_party')    

def compute_similarity(df:pd.DataFrame, lsuffix:str, rsuffix:str,
                       similarity_metric=lambda u,v: 1 - spatial.distance.cosine(u,v)):
    lcols = [f'{v}_{lsuffix}' for v in hp.VOTE_COLS] 
    rcols = [f'{v}_{rsuffix}' for v in hp.VOTE_COLS]
    U = df[lcols].values
    V = df[rcols].values
    df['similarity'] = [similarity_metric(u, v) for u,v in zip(U,V)]
    return df    

def get_mdb_party_similarity(similarity_mdb_party:pd.DataFrame):
    return similarity_mdb_party.groupby('Fraktion/Gruppe')['similarity'].describe().sort_values('mean')    

def plot_similarity_over_time(df:pd.DataFrame, grp_col:str, time_bin:str='y', title:str=None):
    y = 'avg. similarity'
    tmp = (df.groupby([pd.Grouper(key='date', freq=time_bin),grp_col])['similarity'].mean().to_frame(y).reset_index())
    fig = px.line(data_frame=tmp, x='date', y=y, color=grp_col, title=title)
    fig.update_layout(xaxis_title=f'Time [{time_bin}]',
                      yaxis_title=f'{y} (0 = dissimilar, 1 = identical)')
    return fig    

def align_party_with_party(party_votes:pd.DataFrame, 
                           party_a:str, party_b:str):
    tmp = party_votes.reset_index(level=0)
    mask_a = tmp['Fraktion/Gruppe'] == party_a
    mask_b = tmp['Fraktion/Gruppe'] == party_b
    return tmp.loc[mask_a].join(tmp.loc[mask_b], lsuffix='_a', rsuffix='_b').reset_index()

def align_party_with_all_parties(party_votes:pd.DataFrame, party:str):
    similarity_party_party = []
    for party_b in party_votes.index.get_level_values(level='Fraktion/Gruppe').unique():
        if party_b == party: continue
        tmp = align_party_with_party(party_votes, party_a=party, party_b=party_b)
        similarity_party_party.append(tmp)
    similarity_party_party = pd.concat(similarity_party_party, ignore_index=True)
    notna = similarity_party_party['Fraktion/Gruppe_b'].notna()
    return similarity_party_party.loc[notna]    

def get_party_party_similarity(similarity_party_party:pd.DataFrame):
    return similarity_party_party.groupby('Fraktion/Gruppe_b')['similarity'].describe().sort_values('mean')    