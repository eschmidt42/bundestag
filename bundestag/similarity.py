# AUTOGENERATED! DO NOT EDIT! File to edit: nbs/02_member_similarity.ipynb (unless otherwise specified).

__all__ = ['get_squished_dataframe', 'get_agreements_painfully_slow', 'get_dummy', 'scan_all_agreements']

# Cell
import pandas as pd
import typing
import tqdm
import numpy as np
from sklearn.metrics.pairwise import pairwise_distances
import pickle

# Cell
def get_squished_dataframe(df:pd.DataFrame, id_col:str='Bezeichnung',
                                 feature_cols:typing.List[str]=['ja', 'nein', 'Enthaltung', 'ungültig', 'nichtabgegeben'],
                                 topic_cols:typing.List=['date', 'title']):

    tmp = df.loc[:, [id_col]+feature_cols]
    tmp['issue'] = df['date'].apply(str) + ' ' + df['title']

    tmp = tmp.set_index([id_col, 'issue'])
    return (tmp[tmp == 1].stack()
            .reset_index()
            .drop(0,1)
            .rename(columns={'level_2':'vote'}))

# Cell
def get_agreements_painfully_slow(df:pd.DataFrame,
                                  member0:str, member1:str,
                                  verbose:bool=False,
                                  id_col:str='Bezeichnung'):
    #TODO: prettify & speed test the calculation
    members = df[id_col].unique()
    assert member0 in members, f'{member0} not found'
    assert member1 in members, f'{member1} not found'
    res = {}

    member0_mask = df[id_col] == member0
    member1_mask = df[id_col] == member1

    common_issues = set(df.loc[member0_mask,'issue'].values).intersection(df.loc[member1_mask,'issue'].values)

    common_issue_mask = df['issue'].isin(common_issues)
    votes0 = df.loc[member0_mask & common_issue_mask].sort_values('issue')
    votes1 = df.loc[member1_mask & common_issue_mask].sort_values('issue')
    n_issues = df.loc[common_issue_mask,'issue'].nunique()

    if n_issues == 0:
        return res

    agreement_frac = (votes0['vote'].values == votes1['vote'].values).sum() / n_issues
    if verbose: print(f'overall agreement {agreement_frac*100:.2f} %')

    res['overall_frac'] = agreement_frac
    res['overall_total'] = n_issues
    res['member0'] = member0
    res['member1'] = member1

    for outcome in df.loc[common_issue_mask,'vote'].unique():

        n_issues = df.loc[common_issue_mask & (df['vote']==outcome), 'issue'].nunique()
        issues0 = votes0.loc[votes0['vote']==outcome, 'issue'].unique()
        issues1 = votes1.loc[votes1['vote']==outcome, 'issue'].unique()
        n_agree = len(set(issues0).intersection(issues1))
        agreement_frac = n_agree / n_issues
        if verbose: print(f'"{outcome}" agreement {agreement_frac*100:.2f} %')
        res[f'{outcome}_frac'] = agreement_frac
        res[f'{outcome}_total'] = n_issues

    return res

# Cell
def get_dummy(df:pd.DataFrame, mask:pd.Series):
    return (df.loc[mask]
            .assign(dummy=True)
            .pivot_table(index='Bezeichnung', columns='issue', values='dummy', fill_value=False)
            .astype(bool))

def scan_all_agreements(df:pd.DataFrame):
    outcomes = df['vote'].unique()
    agreements = {}
    for outcome in tqdm.tqdm(outcomes, desc='Outcome', total=len(outcomes)):
        mask = df['vote']==outcome
        tmp = get_dummy(df, mask=mask)
        members = tmp.index.values
        similarity = 1 - pairwise_distances(tmp.values, metric='jaccard')
        similarity = 100 * similarity
        agreements[outcome] = pd.DataFrame(similarity, columns=members, index=members)

    tmp = get_dummy(df, df['vote'].notna()).astype(float)
    members = tmp.index.values
    tmp = np.dot(tmp.values, tmp.values.T)
    agreements['total_shared_votes'] = pd.DataFrame(tmp,
                                                    columns=members,
                                                    index=members)

    return agreements