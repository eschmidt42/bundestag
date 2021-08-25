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
- parse data from abgeordnetenwatch $\rightarrow$ `nbs/03_abgeordnetenwatch.ipynb`
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
from bundestag.gui import GUI
from bundestag import abgeordnetenwatch as aw
from bundestag import poll_clustering as pc
from bundestag import vote_prediction as vp

from pathlib import Path
import pandas as pd
from fastai.tabular.all import *
```

### Part 1 - Party/Party similarities and Politician/Party similarities using bundestag.de data

**Loading the data**

If you have cloned the repo you should already have a `roll_call_votes.parquet` file in the root directory of the repo. If not feel free to download the `roll_call_votes.parquet` file directly.

If you want to have a closer look at the preprocessing please check out `nbs/00_html_parsing.ipynb`.


```python
df = pd.read_parquet(path='roll_call_votes.parquet')
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



#### Counting party votes


```python
party_votes, _ = sim.get_votes_by_party(df)
party_votes.head()
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
      <th></th>
      <th>vote</th>
      <th>Enthaltung</th>
      <th>ja</th>
      <th>nein</th>
      <th>nichtabgegeben</th>
      <th>ungültig</th>
    </tr>
    <tr>
      <th>Fraktion/Gruppe</th>
      <th>date</th>
      <th>title</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">AfD</th>
      <th rowspan="5" valign="top">2017-12-12</th>
      <th>Bundeswehreinsatz gegen die Terrororganisation IS</th>
      <td>0.000000</td>
      <td>0.000000</td>
      <td>0.967391</td>
      <td>0.032609</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz im Irak</th>
      <td>0.000000</td>
      <td>0.000000</td>
      <td>0.978261</td>
      <td>0.021739</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz im Mittelmeer (SEA GUARDIAN)</th>
      <td>0.021739</td>
      <td>0.913043</td>
      <td>0.021739</td>
      <td>0.043478</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz in Afghanistan (Resolute Support)</th>
      <td>0.010870</td>
      <td>0.000000</td>
      <td>0.956522</td>
      <td>0.032609</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>Bundeswehreinsatz in Mali (MINUSMA)</th>
      <td>0.000000</td>
      <td>0.000000</td>
      <td>0.967391</td>
      <td>0.032609</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Visualizing similarities of `party` with all other parties over time


```python
%%time
party = 'SPD'
similarity_party_party = (sim.align_party_with_all_parties(party_votes, party)
                          .pipe(sim.compute_similarity, lsuffix='a', rsuffix='b'))
similarity_party_party.head(3).T
```

    CPU times: user 89.9 ms, sys: 0 ns, total: 89.9 ms
    Wall time: 87.8 ms





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
      <th>Enthaltung_a</th>
      <td>0.013072</td>
      <td>0.013072</td>
      <td>0.006536</td>
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
      <th>nichtabgegeben_a</th>
      <td>0.078431</td>
      <td>0.078431</td>
      <td>0.084967</td>
    </tr>
    <tr>
      <th>ungültig_a</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>Fraktion/Gruppe_b</th>
      <td>AfD</td>
      <td>AfD</td>
      <td>AfD</td>
    </tr>
    <tr>
      <th>Enthaltung_b</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.021739</td>
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
      <th>nichtabgegeben_b</th>
      <td>0.032609</td>
      <td>0.021739</td>
      <td>0.043478</td>
    </tr>
    <tr>
      <th>ungültig_b</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
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



Visualize as a time series


```python
sim.plot_similarity_over_time(similarity_party_party, 
                              'Fraktion/Gruppe_b',
                              title=f'{party} vs time')
```

![party similarity](./README_files/party_similarity_vs_time.png)

Politicians (MdB = Mitglied des Bundestages $\rightarrow$ `mdb`) can also be compared agains party votes


```python
%%time
mdb = 'Peter Altmaier'
similarity_mdb_party = (df
                        .pipe(sim.prepare_votes_of_mdb, mdb=mdb)
                        .pipe(sim.align_mdb_with_parties, party_votes=party_votes)
                        .pipe(sim.compute_similarity, lsuffix='mdb', rsuffix='party')
                       )
similarity_mdb_party.head(3).T
```

    CPU times: user 111 ms, sys: 19.2 ms, total: 130 ms
    Wall time: 129 ms





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
      <th>Enthaltung_party</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
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
      <th>nichtabgegeben_party</th>
      <td>0.132353</td>
      <td>0.084388</td>
      <td>0.210526</td>
    </tr>
    <tr>
      <th>ungültig_party</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
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




```python
sim.plot_similarity_over_time(similarity_mdb_party, 
                              'Fraktion/Gruppe',
                              title=f'{mdb} vs time')
```

![mdb similarity](./README_files/mdb_similarity_vs_time.png)

**GUI to inspect similarities**

To make this exploration more convenient, the class `GUI` was implemented to quickly go through the different parties and politicians


```python
GUI(df).render()
```

### Part 2 - predicting politician votes using abgeordnetenwatch data

The data used below was processed using `nbs/03_abgeordnetenwatch.ipynb`.


```python
legislature_id = 111
aw.ABGEORDNETENWATCH_PATH = Path('./abgeordnetenwatch_data')
```

#### Clustering polls using Latent Dirichlet Allocation (LDA)


```python
%%time
source_col = 'poll_title'
nlp_col = f'{source_col}_nlp_processed'
num_topics = 5 # number of topics / clusters to identify

st = pc.SpacyTransformer()

# load data and prepare text for modelling
df_polls_lda = (aw.get_polls_df(legislature_id)
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
      <td>0.066793</td>
      <td>0.050012</td>
      <td>0.033369</td>
    </tr>
    <tr>
      <th>nlp_dim1</th>
      <td>0.06668</td>
      <td>0.799959</td>
      <td>0.033362</td>
    </tr>
    <tr>
      <th>nlp_dim2</th>
      <td>0.066783</td>
      <td>0.050011</td>
      <td>0.033492</td>
    </tr>
    <tr>
      <th>nlp_dim3</th>
      <td>0.733067</td>
      <td>0.050008</td>
      <td>0.034484</td>
    </tr>
  </tbody>
</table>
</div>


    CPU times: user 2.05 s, sys: 175 ms, total: 2.23 s
    Wall time: 2.23 s



```python
pc.pca_plot_lda_topics(df_polls_lda, st, source_col, nlp_feature_cols)
```

#### Predicting votes

Loading data


```python
all_votes_path = aw.ABGEORDNETENWATCH_PATH / f'compiled_votes_legislature_{legislature_id}.csv'

# reading data frame with vote data from disk which was generated by aw.compile_votes_data
df_all_votes = pd.read_csv(all_votes_path) 

# minor pre-processing
df_all_votes = df_all_votes.assign(**{'politician name':vp.get_politician_names})

# loading info on mandates (party association) and polls (titles and descriptions)
df_mandates = aw.get_mandates_df(legislature_id)
df_mandates['party'] = df_mandates.apply(vp.get_party_from_fraction_string, axis=1)
df_polls = aw.get_polls_df(legislature_id)
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






<style type="text/css">
#T_1f33c_row0_col0, #T_1f33c_row1_col1, #T_1f33c_row2_col1, #T_1f33c_row3_col3 {
  background-color: #023858;
  color: #f1f1f1;
}
#T_1f33c_row0_col1, #T_1f33c_row1_col0, #T_1f33c_row2_col0, #T_1f33c_row3_col0 {
  background-color: #fff7fb;
  color: #000000;
}
#T_1f33c_row0_col2 {
  background-color: #fef6fa;
  color: #000000;
}
#T_1f33c_row0_col3, #T_1f33c_row1_col2, #T_1f33c_row1_col3, #T_1f33c_row3_col2 {
  background-color: #fdf5fa;
  color: #000000;
}
#T_1f33c_row2_col2 {
  background-color: #71a8ce;
  color: #f1f1f1;
}
#T_1f33c_row2_col3 {
  background-color: #034a74;
  color: #f1f1f1;
}
#T_1f33c_row3_col1 {
  background-color: #fef6fb;
  color: #000000;
}
</style>
<table id="T_1f33c_">
  <thead>
    <tr>
      <th class="index_name level0" >vote_pred</th>
      <th class="col_heading level0 col0" >abstain</th>
      <th class="col_heading level0 col1" >no</th>
      <th class="col_heading level0 col2" >no_show</th>
      <th class="col_heading level0 col3" >yes</th>
    </tr>
    <tr>
      <th class="index_name level0" >vote</th>
      <th class="blank col0" >&nbsp;</th>
      <th class="blank col1" >&nbsp;</th>
      <th class="blank col2" >&nbsp;</th>
      <th class="blank col3" >&nbsp;</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th id="T_1f33c_level0_row0" class="row_heading level0 row0" >abstain</th>
      <td id="T_1f33c_row0_col0" class="data row0 col0" >803</td>
      <td id="T_1f33c_row0_col1" class="data row0 col1" >50</td>
      <td id="T_1f33c_row0_col2" class="data row0 col2" >58</td>
      <td id="T_1f33c_row0_col3" class="data row0 col3" >64</td>
    </tr>
    <tr>
      <th id="T_1f33c_level0_row1" class="row_heading level0 row1" >no</th>
      <td id="T_1f33c_row1_col0" class="data row1 col0" >24</td>
      <td id="T_1f33c_row1_col1" class="data row1 col1" >9405</td>
      <td id="T_1f33c_row1_col2" class="data row1 col2" >190</td>
      <td id="T_1f33c_row1_col3" class="data row1 col3" >138</td>
    </tr>
    <tr>
      <th id="T_1f33c_level0_row2" class="row_heading level0 row2" >no_show</th>
      <td id="T_1f33c_row2_col0" class="data row2 col0" >103</td>
      <td id="T_1f33c_row2_col1" class="data row2 col1" >978</td>
      <td id="T_1f33c_row2_col2" class="data row2 col2" >546</td>
      <td id="T_1f33c_row2_col3" class="data row2 col3" >917</td>
    </tr>
    <tr>
      <th id="T_1f33c_level0_row3" class="row_heading level0 row3" >yes</th>
      <td id="T_1f33c_row3_col0" class="data row3 col0" >15</td>
      <td id="T_1f33c_row3_col1" class="data row3 col1" >68</td>
      <td id="T_1f33c_row3_col2" class="data row3 col2" >168</td>
      <td id="T_1f33c_row3_col3" class="data row3 col3" >10954</td>
    </tr>
  </tbody>
</table>




<style type="text/css">
#T_e56fd_row0_col0, #T_e56fd_row1_col1, #T_e56fd_row2_col1, #T_e56fd_row3_col3 {
  background-color: #023858;
  color: #f1f1f1;
}
#T_e56fd_row0_col1, #T_e56fd_row1_col0, #T_e56fd_row2_col0, #T_e56fd_row3_col0 {
  background-color: #fff7fb;
  color: #000000;
}
#T_e56fd_row0_col2 {
  background-color: #fef6fa;
  color: #000000;
}
#T_e56fd_row0_col3, #T_e56fd_row1_col2, #T_e56fd_row1_col3, #T_e56fd_row3_col2 {
  background-color: #fdf5fa;
  color: #000000;
}
#T_e56fd_row2_col2 {
  background-color: #71a8ce;
  color: #f1f1f1;
}
#T_e56fd_row2_col3 {
  background-color: #034a74;
  color: #f1f1f1;
}
#T_e56fd_row3_col1 {
  background-color: #fef6fb;
  color: #000000;
}
</style>
<table id="T_e56fd_">
  <thead>
    <tr>
      <th class="index_name level0" >vote_pred</th>
      <th class="col_heading level0 col0" >abstain</th>
      <th class="col_heading level0 col1" >no</th>
      <th class="col_heading level0 col2" >no_show</th>
      <th class="col_heading level0 col3" >yes</th>
    </tr>
    <tr>
      <th class="index_name level0" >vote</th>
      <th class="blank col0" >&nbsp;</th>
      <th class="blank col1" >&nbsp;</th>
      <th class="blank col2" >&nbsp;</th>
      <th class="blank col3" >&nbsp;</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th id="T_e56fd_level0_row0" class="row_heading level0 row0" >abstain</th>
      <td id="T_e56fd_row0_col0" class="data row0 col0" >0.823590</td>
      <td id="T_e56fd_row0_col1" class="data row0 col1" >0.051282</td>
      <td id="T_e56fd_row0_col2" class="data row0 col2" >0.059487</td>
      <td id="T_e56fd_row0_col3" class="data row0 col3" >0.065641</td>
    </tr>
    <tr>
      <th id="T_e56fd_level0_row1" class="row_heading level0 row1" >no</th>
      <td id="T_e56fd_row1_col0" class="data row1 col0" >0.002460</td>
      <td id="T_e56fd_row1_col1" class="data row1 col1" >0.963923</td>
      <td id="T_e56fd_row1_col2" class="data row1 col2" >0.019473</td>
      <td id="T_e56fd_row1_col3" class="data row1 col3" >0.014144</td>
    </tr>
    <tr>
      <th id="T_e56fd_level0_row2" class="row_heading level0 row2" >no_show</th>
      <td id="T_e56fd_row2_col0" class="data row2 col0" >0.040487</td>
      <td id="T_e56fd_row2_col1" class="data row2 col1" >0.384434</td>
      <td id="T_e56fd_row2_col2" class="data row2 col2" >0.214623</td>
      <td id="T_e56fd_row2_col3" class="data row2 col3" >0.360456</td>
    </tr>
    <tr>
      <th id="T_e56fd_level0_row3" class="row_heading level0 row3" >yes</th>
      <td id="T_e56fd_row3_col0" class="data row3 col0" >0.001339</td>
      <td id="T_e56fd_row3_col1" class="data row3 col1" >0.006069</td>
      <td id="T_e56fd_row3_col2" class="data row3 col2" >0.014993</td>
      <td id="T_e56fd_row3_col3" class="data row3 col3" >0.977599</td>
    </tr>
  </tbody>
</table>



    2021-08-25 16:36:02.574 | INFO     | bundestag.vote_prediction:plot_predictions:100 - Overall accuracy = 88.67 %


    
    5 most inaccurately predicted politicians:



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
      <th>politician name</th>
      <th>party</th>
      <th>prediction_correct</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mario Mieruch</td>
      <td>fraktionslos</td>
      <td>0.333333</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Heiko Heßenkemper</td>
      <td>fraktionslos</td>
      <td>0.450000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Thomas Nord</td>
      <td>DIE LINKE</td>
      <td>0.463415</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Axel Troost</td>
      <td>DIE LINKE</td>
      <td>0.500000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Katarina Barley</td>
      <td>SPD</td>
      <td>0.500000</td>
    </tr>
  </tbody>
</table>
</div>


    
    5 most inaccurately predicted polls:



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
      <th>poll_id</th>
      <th>poll_title</th>
      <th>prediction_correct</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1761</td>
      <td>Organspenden-Reform: Zustimmungslösung</td>
      <td>0.593750</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1758</td>
      <td>Organspenden-Reform: Widerspruchslösung</td>
      <td>0.596899</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3572</td>
      <td>Corona-Maßnahmen: Aussetzung der Schuldenbremse - erster Nachtragshaushalt</td>
      <td>0.726708</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1683</td>
      <td>BDS-Bewegung entgegentreten - Antisemitismus bekämpfen</td>
      <td>0.745342</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3571</td>
      <td>Fortsetzung des Bundeswehreinsatzes in Afghanistan</td>
      <td>0.753623</td>
    </tr>
  </tbody>
</table>
</div>


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
