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

## `bundestag` command line interface to process data

For an overview over commands run
```shell
bundestag --help
```

To download data from abgeordnetenwatch, for a specific legislature id
```shell
bundestag download abgeordnetenwatch 132
```

To transform the downloaded data run
```shell
bundestag transform abgeordnetenwatch 132
````

To find out the legislature id for the current Bundestag, visit [abgeordnetenwatch.de](https://www.abgeordnetenwatch.de/bundestag) and click on the "Open Data" button at the bottom of the page.

### When developing

See
```shell
make help
```

for implemented commands to manage the project.

To get started run
```shell
make venv
make install-dev
```

### Docs

For detailed explanations see:
- parse data from bundestag.de $\rightarrow$ `nbs/00_html_parsing.ipynb`
- parse data from abgeordnetenwatch.de $\rightarrow$ `nbs/03_abgeordnetenwatch.ipynb`
- analyze party / abgeordneten similarity $\rightarrow$ `nbs/01_similarities.ipynb`
- cluster polls $\rightarrow$ `nbs/04_poll_clustering.ipynb`
- predict politician votes $\rightarrow$ `nbs/05_predicting_votes.ipynb`

For a short overview of the highlights see below.

### Data sources

* Bundestag page `https://www.bundestag.de/parlament/plenum/abstimmung/liste`: Roll call votes with information on presence / absence and vote (yes/no/abstain) for each member of parliament
* abgeordnetenwatch API `https://www.abgeordnetenwatch.de/api` (they also have a GUI [here](https://www.abgeordnetenwatch.de)): information on politicians, parliaments, legislative periods and mandates

### Setup


```python
%load_ext autoreload
%autoreload 2
```


```python
from pathlib import Path

import pandas as pd
from fastai.tabular.all import *

from bundestag import abgeordnetenwatch as aw
from bundestag import html_parsing as hp
from bundestag import poll_clustering as pc
from bundestag import similarity as sim
from bundestag import vote_prediction as vp
from bundestag.gui import MdBGUI, PartyGUI
```

### Part 1 - Party/Party similarities and Politician/Party similarities using bundestag.de data

**Loading the data**

If you have cloned the repo you should already have a `bundestag.de_votes.parquet` file in the root directory of the repo. If not feel free to download that file directly.

If you want to have a closer look at the preprocessing please check out `nbs/00_html_parsing.ipynb`.


```python
df = pd.read_parquet(path="bundestag.de_votes.parquet")
df.head(3).T
```

Votes by party


```python
%%time
party_votes = sim.get_votes_by_party(df)
sim.test_party_votes(party_votes)
```

Re-arranging `party_votes`


```python
%%time
party_votes_pivoted = sim.pivot_party_votes_df(party_votes)
sim.test_party_votes_pivoted(party_votes_pivoted)
party_votes_pivoted.head()
```

**Similarity of a single politician with the parties**

Collecting the politicians votes


```python
%%time
mdb = "Peter Altmaier"
mdb_votes = sim.prepare_votes_of_mdb(df, mdb)
sim.test_votes_of_mdb(mdb_votes)
mdb_votes.head()
```

Comparing the politician against the parties


```python
%%time
mdb_vs_parties = sim.align_mdb_with_parties(
    mdb_votes, party_votes_pivoted
).pipe(sim.compute_similarity, lsuffix="mdb", rsuffix="party")
sim.test_mdb_vs_parties(mdb_vs_parties)
mdb_vs_parties.head(3).T
```

Plotting


```python
sim.plot(
    mdb_vs_parties,
    title_overall=f"Overall similarity of {mdb} with all parties",
    title_over_time=f"{mdb} vs time",
)
plt.tight_layout()
plt.show()
```

![mdb similarity](./README_files/mdb_similarity_vs_time.png)

**Comparing one specific party against all others**

Collecting party votes


```python
%%time
party = "SPD"
partyA_vs_rest = sim.align_party_with_all_parties(
    party_votes_pivoted, party
).pipe(sim.compute_similarity, lsuffix="a", rsuffix="b")
sim.test_partyA_vs_partyB(partyA_vs_rest)
partyA_vs_rest.head(3).T
```

Plotting


```python
sim.plot(
    partyA_vs_rest,
    title_overall=f"Overall similarity of {party} with all parties",
    title_over_time=f"{party} vs time",
    party_col="Fraktion/Gruppe_b",
)
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
path = Path("./abgeordnetenwatch_data")
```

#### Clustering polls using Latent Dirichlet Allocation (LDA)


```python
%%time
source_col = "poll_title"
nlp_col = f"{source_col}_nlp_processed"
num_topics = 5  # number of topics / clusters to identify

st = pc.SpacyTransformer()

# load data and prepare text for modelling
df_polls_lda = pd.read_parquet(path=path / "df_polls.parquet").assign(
    **{nlp_col: lambda x: st.clean_text(x, col=source_col)}
)

# modelling clusters
st.fit(df_polls_lda[nlp_col].values, mode="lda", num_topics=num_topics)

# creating text features using fitted model
df_polls_lda, nlp_feature_cols = df_polls_lda.pipe(
    st.transform, col=nlp_col, return_new_cols=True
)

# inspecting clusters
display(df_polls_lda.head(3).T)
```


```python
pc.pca_plot_lda_topics(df_polls_lda, st, source_col, nlp_feature_cols)
```

#### Predicting votes

Loading data


```python
df_all_votes = pd.read_parquet(path=path / "df_all_votes.parquet")
df_mandates = pd.read_parquet(path=path / "df_mandates.parquet")
df_polls = pd.read_parquet(path=path / "df_polls.parquet")
```

Splitting data set into training and validation set. Splitting randomly here because it leads to an interesting result, albeit not very realistic for production.


```python
splits = RandomSplitter(valid_pct=0.2)(df_all_votes)
y_col = "vote"
```

Training a neural net to predict `vote` based on embeddings for `poll_id` and `politician name`


```python
%%time
to = TabularPandas(
    df_all_votes,
    cat_names=[
        "politician name",
        "poll_id",
    ],  # columns in `df_all_votes` to treat as categorical
    y_names=[y_col],  # column to use as a target for the model in `learn`
    procs=[Categorify],  # processing of features
    y_block=CategoryBlock,  # how to treat `y_names`, here as categories
    splits=splits,
)  # how to split the data

dls = to.dataloaders(bs=512)
learn = tabular_learner(
    dls
)  # fastai function to set up a neural net for tabular data
lrs = learn.lr_find()  # searches the learning rate
learn.fit_one_cycle(
    5, lrs.valley
)  # performs training using one-cycle hyperparameter schedule
```

**Predictions over unseen data**

Inspecting the predictions of the neural net over the validation set.


```python
vp.plot_predictions(
    learn, df_all_votes, df_mandates, df_polls, splits, n_worst_politicians=5
)
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
vp.plot_poll_embeddings(
    df_all_votes, df_polls, embeddings, df_mandates=df_mandates
)
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
