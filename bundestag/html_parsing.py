from bs4 import BeautifulSoup
from pathlib import Path
import re, os
import typing
import requests
import tqdm
import time
import pandas as pd
import itertools
from fastcore.all import *
from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")
# default level for this module should be INFO

RE_HTM = re.compile('(\.html?)')
RE_FNAME = re.compile('(\.xlsx?)')
RE_SHEET = re.compile('(XLSX?)')
VOTE_COLS = ['ja', 'nein', 'Enthaltung', 'ungültig', 'nichtabgegeben']

def get_file_paths(path:typing.Union[Path,str], suffix:str=None, pattern=None): 
    'Collecting files with a specific suffix or pattern from `path`'
    return Path(path).ls().filter(lambda x: x.suffix=='.htm' or (pattern and pattern.search(str(x)))).unique()

def test_file_paths(html_file_paths:list, html_path:Path):
    assert len(html_file_paths) > len(get_file_paths(html_path, suffix='.htm'))

def collect_sheet_uris(html_file_paths:typing.List[Path]):
    uris = {}
    pattern = re.compile('(XLSX?)')

    for file_path in tqdm.tqdm(html_file_paths, total=len(html_file_paths), desc='HTM(L)'):
        
        with open(file_path, 'r') as f:
            soup = BeautifulSoup(f, features="html.parser")
        
        elements = soup.find_all('td', attrs={'data-th':'Dokument'})
        for element in elements:
            title = element.div.p.strong.text.strip()
            href = element.find('a', attrs={'title':RE_SHEET})
            if href is None: continue
            uris[title] = href['href']
    return uris

def test_sheet_uris(sheet_uris):
    assert isinstance(sheet_uris, dict)
    assert '10.09.2020: Abstrakte Normenkontrolle - Düngeverordnung (Beschlussempfehlung)' in sheet_uris

def download_sheet(uri:str, sheet_path:typing.Union[Path,str],
                   verbose:bool=False):
    sheet_path.mkdir(exist_ok=True)
    path = Path(sheet_path) / uri.split('/')[-1]
    with open(path, 'wb') as f:
        r = requests.get(uri)
        if verbose: print(f'Writing to {path}')
        f.write(r.content)    

def download_multiple_sheets(uris:typing.Dict[str,str], sheet_path:typing.Union[Path,str],
                             t_sleep:float=.01, nmax:int=None):
    file_title_maps = {uri.split('/')[-1]: title for title, uri in uris.items()}
    for i, (title, uri) in tqdm.tqdm(enumerate(uris.items()), desc='File', total=len(uris)):
        if nmax is not None and i > nmax: break
        if (Path(sheet_path) / uri.split('/')[-1]).exists(): continue
        download_sheet(uri, sheet_path=sheet_path)
        time.sleep(t_sleep)
    return file_title_maps        

def test_file_title_maps(file_title_maps, sheet_uris):
    assert len(file_title_maps) == len(sheet_uris)    

def is_date(s:str, fun:typing.Callable):
    try:
        _ = fun(s)
        return True
    except:
        return False
    
def get_sheet_df(sheet_file:typing.Union[str,Path], file_title_maps:typing.Dict[str,str]=None): 
    'Parsing xlsx and xls files into dataframes'
    
    if Path(sheet_file).stat().st_size == 0:
        logger.warning(f'{sheet_file} is of size 0, skipping ...')
        return
    
    dfs = pd.read_excel(sheet_file, sheet_name=None)
    
    assert len(dfs) == 1, 'The sheet file has more than one page, that\'s unexpected.'
    
    for name, df in dfs.items():
        df['sheet_name'] = name
    
    assert not (df[VOTE_COLS].sum(axis=1) == 0).any()
    assert not (df[VOTE_COLS].sum(axis=1) > 1).any()
    
    date, title = None, None
    if file_title_maps is not None:    
        title, date = handle_title_and_date(file_title_maps[sheet_file.name], sheet_file)
        
    df['date'] = date
    df['title'] = title
        
    return df.pipe(disambiguate_party)

def test_get_sheet_df(df:pd.DataFrame):
    assert isinstance(df, pd.DataFrame)
    assert all([col in df.columns.values for col in  ['Wahlperiode', 'Sitzungnr', 'Abstimmnr', 'Fraktion/Gruppe', 'Name',
                                                    'Vorname', 'Titel', 'ja', 'nein', 'Enthaltung', 'ungültig', 'nichtabgegeben', 
                                                    'Bezeichnung', 'sheet_name', 'date', 'title']])

def handle_title_and_date(full_title:str, sheet_file):
    'Extracting the title of the roll call vote and the date'
    title = full_title.split(':')
    date = title[0]
    if is_date(date, lambda x: pd.to_datetime(x, dayfirst=True)):
        date = pd.to_datetime(date, dayfirst=True)
        title = ':'.join(title[1:])
    elif is_date(sheet_file.name.split('_')[0], pd.to_datetime):
        date = pd.to_datetime(sheet_file.name.split('_')[0])
        title = full_title
    else:
        date = None
        title = full_title
    return title, date

PARTY_MAP = {'BÜNDNIS`90/DIE GRÜNEN': 'BÜ90/GR', 'DIE LINKE': 'DIE LINKE.', 'fraktionslos': 'Fraktionslos', 'fraktionslose': 'Fraktionslos'}

def disambiguate_party(df:pd.DataFrame, col:str='Fraktion/Gruppe', party_map:dict=None):
    if party_map is None: party_map = PARTY_MAP
    df[col] = df[col].apply(lambda x: x if x not in party_map else party_map[x])
    return df    

def get_squished_dataframe(df:pd.DataFrame, id_col:str='Bezeichnung',
                           feature_cols:typing.List[str]=VOTE_COLS,
                           other_cols:typing.List=None):
    
    other_cols = ['date', 'title'] if other_cols is None else other_cols
    tmp = df.loc[:, [id_col] + feature_cols + other_cols]
    tmp['issue'] = df['date'].dt.date.apply(str) + ' ' + df['title']

    tmp = tmp.set_index([id_col, 'issue'] + other_cols)
    tmp = (tmp[tmp == 1].stack()
            .reset_index()
            .drop(0,1)
            .rename(columns={f'level_{2+len(other_cols)}':'vote'}))
    return df.join(tmp.set_index(['Bezeichnung','date','title']), on=['Bezeichnung','date','title']).drop(columns=VOTE_COLS)    

def test_squished_df(df_squished:pd.DataFrame, df:pd.DataFrame):
    assert len(df_squished) == len(df)
    assert 'vote' in df_squished.columns
    assert 'issue' in df_squished.columns
    assert not any([v in df_squished.columns for v in VOTE_COLS])

DTYPES = {'Wahlperiode':int, 'Sitzungnr': int, 'Abstimmnr': int, 'Fraktion/Gruppe':str, 
          'Name': str, 'Vorname': str, 'Titel': str, 'vote': str, 'issue': str,
#           'ja': bool, 'nein': bool, 'Enthaltung': bool, 'ungültig': bool, 'nichtabgegeben': bool, 'Bemerkung': str,
          'Bezeichnung': str, 'sheet_name': str,
          'date': 'datetime64[ns]', 'title':str}

def set_sheet_dtypes(df:pd.DataFrame):
    for col, dtype in DTYPES.items():
        df[col] = df[col].astype(dtype)
    return df    

def get_multiple_sheets_df(sheet_files:typing.List[typing.Union[Path,str]],
                           file_title_maps:typing.Dict[str,str]=None): 
    df = []
    for sheet_file in tqdm.tqdm(sheet_files, total=len(sheet_files), desc='Sheets'):
        df.append(
            (get_sheet_df(sheet_file, file_title_maps=file_title_maps)
             .pipe(get_squished_dataframe)
             .pipe(set_sheet_dtypes))
        )
    return pd.concat(df, ignore_index=True)    

def get_multiple_sheets(html_path, sheet_path, nmax:int=None):
    html_path, sheet_path = Path(html_path), Path(sheet_path)
    html_file_paths = get_file_paths(html_path, pattern=RE_HTM)
    sheet_uris = collect_sheet_uris(html_file_paths)
    file_title_maps = download_multiple_sheets(sheet_uris, sheet_path=sheet_path, nmax=nmax)
    sheet_files = get_file_paths(sheet_path, pattern=RE_FNAME)
    df = get_multiple_sheets_df(sheet_files, file_title_maps=file_title_maps)
    return df    