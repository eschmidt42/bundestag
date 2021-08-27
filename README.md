# "Namentliche Abstimmungen"  in the Bundestag

> Parse and inspect "Namentliche Abstimmungen" (roll call votes) in the Bundestag (the federal German parliament)

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/eschmidt42/bundestag/HEAD)

## Context

The German Parliament is so friendly to put all votes of all members into readable XLSX / XLS files (and PDFs ¯\\\_(ツ)\_/¯ ). Those files  can be found here: https://www.bundestag.de/parlament/plenum/abstimmung/liste. 

Furthermore, the organisation [abgeordnetenwatch](https://www.abgeordnetenwatch.de/) offers a great platform to get to know the individual politicians and their behavior as well as an [open API](https://www.abgeordnetenwatch.de/api) to request data.

## Purpose of this repo

The purpose of this repo is to help collect roll call votes from the parliament's site directly or via abgeordnetenwatch's API and make them available for analysis / modelling. This may be particularly interesting for the upcoming election in 2021. E.g., if you want to see what your local member of the parliament has been up to in terms of public roll call votes relative to the parties, or how individual parties agree in their votes, this dataset may be interesting for you. 

Since the files on the bundestag website are stored in a way making it tricky to automatically crawl them, a bit of manual work is required to generate that dataset. But don't fret! Quite a few recent roll call votes (as of the publishing of this repo) are already prepared for you. But if older or more recent roll call votes are missing, convenience tools to reduce your manual effort are demonstrated below. An alternative route to get the same and more data (on politicians and local parliaments as well) is via the abgeordnetenwatch route.

For your inspiration, I have also included an analysis on how similar parties voted / how similar to parties individual MdBs votes and a small machine learning model which predicts the individual votes of parliament. Teaser: the "fraktionsszwang" seems to exist but is not absolute and the data shows 😁.

## How to install

`pip install bundestag`

## How to use

For detailed explanations see:
- parse data from bundestag.de $\rightarrow$ `nbs/00_html_parsing.ipynb`
- parse data from abgeordnetenwatch.de $\rightarrow$ `nbs/03_abgeordnetenwatch.ipynb`
- analyze party / abgeordneten similarity $\rightarrow$ `nbs/01_similarities.ipynb`
- cluster polls $\rightarrow$ `nbs/04_poll_clustering.ipynb`
- predict politician votes $\rightarrow$ `nbs/05_predicting_votes.ipynb`

For a short overview of the highlights see below.

### Setup


```python
%load_ext autoreload
%autoreload 2
```


```python
from bundestag import html_parsing as hp
from bundestag import similarity as sim
from bundestag.gui import MdBGUI, PartyGUI
from bundestag import abgeordnetenwatch as aw
from bundestag import poll_clustering as pc
from bundestag import vote_prediction as vp

from pathlib import Path
import pandas as pd
from fastai.tabular.all import *
```

### Part 1 - Party/Party similarities and Politician/Party similarities using bundestag.de data

**Loading the data**

If you have cloned the repo you should already have a `bundestag.de_votes.parquet` file in the root directory of the repo. If not feel free to download that file directly.

If you want to have a closer look at the preprocessing please check out `nbs/00_html_parsing.ipynb`.


```python
df = pd.read_parquet(path='bundestag.de_votes.parquet')
df.head(3).T
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>1</th>
      <th>2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>Wahlperiode</th>
      <td>17</td>
      <td>17</td>
      <td>17</td>
    </tr>
    <tr>
      <th>Sitzungnr</th>
      <td>198</td>
      <td>198</td>
      <td>198</td>
    </tr>
    <tr>
      <th>Abstimmnr</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>Fraktion/Gruppe</th>
      <td>CDU/CSU</td>
      <td>CDU/CSU</td>
      <td>CDU/CSU</td>
    </tr>
    <tr>
      <th>Name</th>
      <td>Aigner</td>
      <td>Altmaier</td>
      <td>Aumer</td>
    </tr>
    <tr>
      <th>Vorname</th>
      <td>Ilse</td>
      <td>Peter</td>
      <td>Peter</td>
    </tr>
    <tr>
      <th>Titel</th>
      <td>nan</td>
      <td>nan</td>
      <td>nan</td>
    </tr>
    <tr>
      <th>Bezeichnung</th>
      <td>Ilse Aigner</td>
      <td>Peter Altmaier</td>
      <td>Peter Aumer</td>
    </tr>
    <tr>
      <th>sheet_name</th>
      <td>T_Export</td>
      <td>T_Export</td>
      <td>T_Export</td>
    </tr>
    <tr>
      <th>date</th>
      <td>2012-10-18 00:00:00</td>
      <td>2012-10-18 00:00:00</td>
      <td>2012-10-18 00:00:00</td>
    </tr>
    <tr>
      <th>title</th>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
    </tr>
    <tr>
      <th>issue</th>
      <td>2012-10-18  Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>2012-10-18  Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>2012-10-18  Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
    </tr>
    <tr>
      <th>vote</th>
      <td>ja</td>
      <td>nichtabgegeben</td>
      <td>ja</td>
    </tr>
    <tr>
      <th>AbgNr</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>Bemerkung</th>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



Votes by party


```python
%%time
party_votes = sim.get_votes_by_party(df)
sim.test_party_votes(party_votes)
```

    2021-08-27 06:53:37.585 | INFO     | bundestag.similarity:get_votes_by_party:17 - Computing votes by party and poll


    CPU times: user 5.4 s, sys: 0 ns, total: 5.4 s
    Wall time: 5.38 s


Re-arranging `party_votes`


```python
%%time
party_votes_pivoted = sim.pivot_party_votes_df(party_votes)
sim.test_party_votes_pivoted(party_votes_pivoted)
party_votes_pivoted.head()
```

    CPU times: user 19 s, sys: 504 ms, total: 19.5 s
    Wall time: 19.5 s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>vote</th>
      <th>Fraktion/Gruppe</th>
      <th>ja</th>
      <th>nein</th>
      <th>Enthaltung</th>
      <th>ungültig</th>
      <th>nichtabgegeben</th>
    </tr>
    <tr>
      <th>date</th>
      <th>title</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">2017-12-12</th>
      <th>Bundeswehreinsatz gegen die Terrororganisation IS</th>
      <td>AfD</td>
      <td>0.000000</td>
      <td>0.967391</td>
      <td>0.000000</td>
      <td>0</td>
      <td>0.032609</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz im Irak</th>
      <td>AfD</td>
      <td>0.000000</td>
      <td>0.978261</td>
      <td>0.000000</td>
      <td>0</td>
      <td>0.021739</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz im Mittelmeer (SEA GUARDIAN)</th>
      <td>AfD</td>
      <td>0.913043</td>
      <td>0.021739</td>
      <td>0.021739</td>
      <td>0</td>
      <td>0.043478</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz in Afghanistan (Resolute Support)</th>
      <td>AfD</td>
      <td>0.000000</td>
      <td>0.956522</td>
      <td>0.010870</td>
      <td>0</td>
      <td>0.032609</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz in Mali (MINUSMA)</th>
      <td>AfD</td>
      <td>0.000000</td>
      <td>0.967391</td>
      <td>0.000000</td>
      <td>0</td>
      <td>0.032609</td>
    </tr>
  </tbody>
</table>
</div>



**Similarity of a single politician with the parties**

Collecting the politicians votes


```python
%%time
mdb = 'Peter Altmaier'
mdb_votes = sim.prepare_votes_of_mdb(df, mdb)
sim.test_votes_of_mdb(mdb_votes)
mdb_votes.head()
```

    CPU times: user 62.9 ms, sys: 249 µs, total: 63.2 ms
    Wall time: 61.7 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>title</th>
      <th>ja</th>
      <th>nein</th>
      <th>Enthaltung</th>
      <th>ungültig</th>
      <th>nichtabgegeben</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>2012-10-18</td>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>621</th>
      <td>2012-10-25</td>
      <td>17/10059 und 17/11093, Abkommen zwischen Deutschland und der Schweiz</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1241</th>
      <td>2012-10-25</td>
      <td>17/11172, Änderungsantrag zum Gesetzentwurf zur Stärkung der deutschen Finanzaufsicht</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1861</th>
      <td>2012-10-25</td>
      <td>17/11193, Änderungsantrag zum Jahressteuergesetz 2013</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2481</th>
      <td>2012-10-25</td>
      <td>17/11196, Änderungsantrag zum Jahressteuergesetz 2013</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



Comparing the politician against the parties


```python
%%time
mdb_vs_parties = (sim.align_mdb_with_parties(mdb_votes, party_votes_pivoted)
                  .pipe(sim.compute_similarity, lsuffix='mdb', rsuffix='party'))
sim.test_mdb_vs_parties(mdb_vs_parties)
mdb_vs_parties.head(3).T
```

    2021-08-27 06:54:02.682 | INFO     | bundestag.similarity:compute_similarity:110 - Computing similarities using `lsuffix` = "mdb", `rsuffix` = "party" and metric = <function cosine_similarity at 0x7fb7e220e0d0>


    CPU times: user 81.3 ms, sys: 1.03 ms, total: 82.4 ms
    Wall time: 77.5 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>1</th>
      <th>1</th>
      <th>1</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>date</th>
      <td>2012-10-18 00:00:00</td>
      <td>2012-10-18 00:00:00</td>
      <td>2012-10-18 00:00:00</td>
    </tr>
    <tr>
      <th>title</th>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
      <td>Gesetzentwurf 17/9852 und 17/11053 (8. Änderung des Gesetzes gegen Wettbewerbsbeschränkungen)</td>
    </tr>
    <tr>
      <th>ja_mdb</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>nein_mdb</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>Enthaltung_mdb</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>ungültig_mdb</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>nichtabgegeben_mdb</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>Fraktion/Gruppe</th>
      <td>BÜ90/GR</td>
      <td>CDU/CSU</td>
      <td>DIE LINKE.</td>
    </tr>
    <tr>
      <th>ja_party</th>
      <td>0.0</td>
      <td>0.915612</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>nein_party</th>
      <td>0.867647</td>
      <td>0.0</td>
      <td>0.789474</td>
    </tr>
    <tr>
      <th>Enthaltung_party</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>ungültig_party</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>nichtabgegeben_party</th>
      <td>0.132353</td>
      <td>0.084388</td>
      <td>0.210526</td>
    </tr>
    <tr>
      <th>similarity</th>
      <td>0.150798</td>
      <td>0.091777</td>
      <td>0.257663</td>
    </tr>
  </tbody>
</table>
</div>



Plotting


```python
sim.plot(mdb_vs_parties, title_overall=f'Overall similarity of {mdb} with all parties',
         title_over_time=f'{mdb} vs time')
plt.tight_layout()
plt.show()
```

![mdb similarity](./README_files/mdb_similarity_vs_time.png)

**Comparing one specific party against all others**

Collecting party votes


```python
%%time
party = 'SPD'
partyA_vs_rest = (sim.align_party_with_all_parties(party_votes_pivoted, party)
                  .pipe(sim.compute_similarity, lsuffix='a', rsuffix='b'))
sim.test_partyA_vs_partyB(partyA_vs_rest)
partyA_vs_rest.head(3).T
```

    2021-08-27 06:54:02.842 | INFO     | bundestag.similarity:compute_similarity:110 - Computing similarities using `lsuffix` = "a", `rsuffix` = "b" and metric = <function cosine_similarity at 0x7fb7e220e0d0>


    CPU times: user 119 ms, sys: 768 µs, total: 120 ms
    Wall time: 109 ms





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>273</th>
      <th>274</th>
      <th>275</th>
    </tr>
    <tr>
      <th>vote</th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>date</th>
      <td>2017-12-12 00:00:00</td>
      <td>2017-12-12 00:00:00</td>
      <td>2017-12-12 00:00:00</td>
    </tr>
    <tr>
      <th>title</th>
      <td>Bundeswehreinsatz gegen die Terrororganisation IS</td>
      <td>Bundeswehreinsatz im Irak</td>
      <td>Bundeswehreinsatz im Mittelmeer (SEA GUARDIAN)</td>
    </tr>
    <tr>
      <th>Fraktion/Gruppe_a</th>
      <td>SPD</td>
      <td>SPD</td>
      <td>SPD</td>
    </tr>
    <tr>
      <th>ja_a</th>
      <td>0.810458</td>
      <td>0.843137</td>
      <td>0.869281</td>
    </tr>
    <tr>
      <th>nein_a</th>
      <td>0.098039</td>
      <td>0.065359</td>
      <td>0.039216</td>
    </tr>
    <tr>
      <th>Enthaltung_a</th>
      <td>0.013072</td>
      <td>0.013072</td>
      <td>0.006536</td>
    </tr>
    <tr>
      <th>ungültig_a</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>nichtabgegeben_a</th>
      <td>0.078431</td>
      <td>0.078431</td>
      <td>0.084967</td>
    </tr>
    <tr>
      <th>Fraktion/Gruppe_b</th>
      <td>AfD</td>
      <td>AfD</td>
      <td>AfD</td>
    </tr>
    <tr>
      <th>ja_b</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.913043</td>
    </tr>
    <tr>
      <th>nein_b</th>
      <td>0.967391</td>
      <td>0.978261</td>
      <td>0.021739</td>
    </tr>
    <tr>
      <th>Enthaltung_b</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.021739</td>
    </tr>
    <tr>
      <th>ungültig_b</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>nichtabgegeben_b</th>
      <td>0.032609</td>
      <td>0.021739</td>
      <td>0.043478</td>
    </tr>
    <tr>
      <th>similarity</th>
      <td>0.12268</td>
      <td>0.078981</td>
      <td>0.998405</td>
    </tr>
  </tbody>
</table>
</div>



Plotting


```python
sim.plot(partyA_vs_rest, title_overall=f'Overall similarity of {party} with all parties',
         title_over_time=f'{party} vs time', party_col='Fraktion/Gruppe_b')
plt.tight_layout()
plt.show()
```

![party similarity](./README_files/party_similarity_vs_time.png)

**GUI to inspect similarities**

To make the above exploration more interactive, the class `MdBGUI` and `PartyGUI` was implemented to quickly go through the different parties and politicians


```python
mdb = MdBGUI(df)
```


```python
mdb.render()
```


```python
party = PartyGUI(df)
```


```python
party.render()
```

### Part 2 - predicting politician votes using abgeordnetenwatch data

The data used below was processed using `nbs/03_abgeordnetenwatch.ipynb`.


```python
path = Path('./abgeordnetenwatch_data')
```

#### Clustering polls using Latent Dirichlet Allocation (LDA)


```python
%%time
source_col = 'poll_title'
nlp_col = f'{source_col}_nlp_processed'
num_topics = 5 # number of topics / clusters to identify

st = pc.SpacyTransformer()

# load data and prepare text for modelling
df_polls_lda = (pd.read_parquet(path=path/'df_polls.parquet')
                .assign(**{nlp_col: lambda x: st.clean_text(x, col=source_col)}))

# modelling clusters
st.fit(df_polls_lda[nlp_col].values, mode='lda', num_topics=num_topics)

# creating text features using fitted model
df_polls_lda, nlp_feature_cols = df_polls_lda.pipe(st.transform, col=nlp_col, return_new_cols=True)

# inspecting clusters
display(df_polls_lda.head(3).T)
```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>1</th>
      <th>2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>poll_id</th>
      <td>4217</td>
      <td>4215</td>
      <td>4214</td>
    </tr>
    <tr>
      <th>poll_title</th>
      <td>Änderung im Infektions­schutz­gesetz</td>
      <td>Keine Verwendung von geschlechtergerechter Sprache</td>
      <td>Verlängerung des Bundeswehreinsatzes vor der libanesischen Küste (UNIFIL 2021/2022)</td>
    </tr>
    <tr>
      <th>poll_first_committee</th>
      <td>Ausschuss für Recht und Verbraucherschutz</td>
      <td>None</td>
      <td>Auswärtiger Ausschuss</td>
    </tr>
    <tr>
      <th>poll_description</th>
      <td>Abgestimmt wurde über die Paragraphen 9 und 10 des Infektionsschutzgesetzes. Die AfD hatte verlangt, über einzelne Teile des Gesetzentwurfs und den Gesetzentwurf insgesamt, getrennt abzustimmen. Eine namentlicher Abstimmung fand lediglich bezüglich der Änderungen des Infektionsschutzgesetzes statt.\nDer Gesetzentwurf wird mit 408 Ja-Stimmen der Fraktionen CDU/CSU, SPD und Bündnis 90/Die Grünen angenommen. Dagegen stimmten die FDP, Die Linke und die AfD.</td>
      <td>Der Bundestag stimmt über einen Antrag der AfD ab, in welchem die Fraktion dazu auffordert, zugunsten einer "besseren Lesbarkeit" auf die Verwendung geschlechtergerechter Sprache durch die Bundesregierung sowie in Drucksachen des Bundestages zu verzichten. \nDer Antrag wurde mit 531 Nein-Stimmen der Fraktionen CDU/CSU, SPD, Bündnis90/Die Grünen, Die Linke und FDP abgelehnt. Dafür stimmte lediglich die antragsstellende Fraktion der AfD.</td>
      <td>Der von der Bundesregierung eingebrachte Antrag sieht vor, die Beteiligung der Bundeswehr am maritimen Teil der friedenssichernden Mission "United Nations Interim Force in Lebanon" (UNIFIL) zu verlängern. Bei dem Einsatz handelt es sich um die Beteiligung deutscher Streitkräfte an der Überwachung der Seegrenzen des Libanon.\nDer Antrag wird mit 468 Ja-Stimmen der Fraktionen CDU/CSU, SPD, FDP und Bündnis 90/Die Grünen angenommen. Die Linke und die AfD stimmten gegen den Antrag.</td>
    </tr>
    <tr>
      <th>legislature_id</th>
      <td>111</td>
      <td>111</td>
      <td>111</td>
    </tr>
    <tr>
      <th>legislature_period</th>
      <td>Bundestag 2017 - 2021</td>
      <td>Bundestag 2017 - 2021</td>
      <td>Bundestag 2017 - 2021</td>
    </tr>
    <tr>
      <th>poll_date</th>
      <td>2021-06-24</td>
      <td>2021-06-24</td>
      <td>2021-06-24</td>
    </tr>
    <tr>
      <th>poll_title_nlp_processed</th>
      <td>[Änderung, Infektions­schutz­gesetz]</td>
      <td>[Verwendung, geschlechtergerechter, Sprache]</td>
      <td>[Verlängerung, Bundeswehreinsatzes, libanesischen, Küste, UNIFIL]</td>
    </tr>
    <tr>
      <th>nlp_dim0</th>
      <td>0.730206</td>
      <td>0.050009</td>
      <td>0.034305</td>
    </tr>
    <tr>
      <th>nlp_dim1</th>
      <td>0.067006</td>
      <td>0.050014</td>
      <td>0.03356</td>
    </tr>
    <tr>
      <th>nlp_dim2</th>
      <td>0.067219</td>
      <td>0.050011</td>
      <td>0.033626</td>
    </tr>
    <tr>
      <th>nlp_dim3</th>
      <td>0.068889</td>
      <td>0.050011</td>
      <td>0.033718</td>
    </tr>
  </tbody>
</table>
</div>


    CPU times: user 1.83 s, sys: 111 ms, total: 1.94 s
    Wall time: 1.94 s



```python
pc.pca_plot_lda_topics(df_polls_lda, st, source_col, nlp_feature_cols)
```

#### Predicting votes

Loading data


```python
df_all_votes = pd.read_parquet(path=path / 'df_all_votes.parquet')
df_mandates = pd.read_parquet(path=path / 'df_mandates.parquet')
df_polls = pd.read_parquet(path=path / 'df_polls.parquet')
```

Splitting data set into training and validation set. Splitting randomly here because it leads to an interesting result, albeit not very realistic for production.


```python
splits = RandomSplitter(valid_pct=.2)(df_all_votes)
y_col = 'vote'
```

Training a neural net to predict `vote` based on embeddings for `poll_id` and `politician name`


```python
%%time
to = TabularPandas(df_all_votes, 
                   cat_names=['politician name', 'poll_id'], # columns in `df_all_votes` to treat as categorical
                   y_names=[y_col], # column to use as a target for the model in `learn`
                   procs=[Categorify],  # processing of features
                   y_block=CategoryBlock,  # how to treat `y_names`, here as categories
                   splits=splits) # how to split the data 

dls = to.dataloaders(bs=512)
learn = tabular_learner(dls) # fastai function to set up a neural net for tabular data
lrs = learn.lr_find() # searches the learning rate
learn.fit_one_cycle(5, lrs.valley) # performs training using one-cycle hyperparameter schedule
```

**Predictions over unseen data**

Inspecting the predictions of the neural net over the validation set. 


```python
vp.plot_predictions(learn, df_all_votes, df_mandates, df_polls, splits,
                    n_worst_politicians=5)
```

Splitting our dataset randomly leads to a surprisingly good accuracy of ~88% over the validation set. The most reasonable explanation is that the model encountered polls and how most politicians voted for them already during training. 

This can be interpreted as, if it is known how most politicians will vote during a poll, then the vote of the remaining politicians is highly predictable. Splitting the data set by `poll_id`, as can be done using `vp.poll_splitter` leads to random chance predictions. Anything else would be surprising as well since the only available information provided to the model is who is voting.

**Visualising learned embeddings**

Besides the actual prediction it also is interesting to inspect what the model actually learned. This can sometimes lead to [surprises](https://github.com/entron/entity-embedding-rossmann).

So let's look at the learned embeddings


```python
embeddings = vp.get_embeddings(learn)
```

To make sense of the embeddings for `poll_id` as well as `politician name` we apply Principal Component Analysis (so one still kind of understands what distances mean) and project down to 2d. 

Using the information which party was most strongly (% of their votes being "yes"), so its strongest proponent, we color code the individual polls.


```python
vp.plot_poll_embeddings(df_all_votes, df_polls, embeddings, df_mandates=df_mandates)
```

![poll embeddings](./README_files/poll_embeddings.png)

The politician embeddings are color coded using the politician's party membership


```python
vp.plot_politician_embeddings(df_all_votes, df_mandates, embeddings)
```

![mandate embeddings](./README_files/mandate_embeddings.png)

The politician embeddings may be the most surprising finding in its clarity. It seems we find for polls and politicians 2-3 clusters, but for politicians with a significant grouping of mandates associated with the government coalition. It seems we find one cluster for the government parties and one for the government opposition. 

## To dos / contributing

Any contributions welcome. In the notebooks in `./nbs/` I've listed to dos here and there things which could be done.

**General to dos**:
- Check for discrepancies between bundestag.de and abgeordnetenwatch based data 
- Make the clustering of polls and policitians interactive
- Extend the vote prediction model: currently, if the data is split by poll (which would be the realistic case when trying to predict votes of a new poll), the model is hardly better than chance. It would be interesting to see which information would help improve beyond chance.
- Extend the data processed from the stored json responses from abgeordnetenwatch (currently only using the bare minimum)


```python

```
