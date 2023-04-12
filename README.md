# "Namentliche Abstimmungen"  in the Bundestag

> Parse and inspect "Namentliche Abstimmungen" (roll call votes) in the Bundestag (the federal German parliament)

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/eschmidt42/bundestag/data?labpath=README.ipynb) [![Tests](https://github.com/eschmidt42/bundestag/actions/workflows/unittests.yml/badge.svg)](https://github.com/eschmidt42/bundestag/actions/workflows/unittests.yml) [![codecov](https://codecov.io/gh/eschmidt42/bundestag/branch/main/graph/badge.svg?token=SIZEIVYX66)](https://codecov.io/gh/eschmidt42/bundestag)

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

### `bundestag` command line interface to process data

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

To download prepared raw and transformed data from huggingface run
```shell
bundestag download huggingface
```

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

To build the docs run
```shell
make docs
```

to create the readme files and
```shell
python3 -m mkdocs serve
```

to test out the autogenerated documentation that will be deployed to github pages.

## Notebooks / analyses

For detailed explanations see:
- parse data from bundestag.de $\rightarrow$ `nbs/00_html_parsing.ipynb`
- parse data from abgeordnetenwatch.de $\rightarrow$ `nbs/03_abgeordnetenwatch.ipynb`
- analyze party / abgeordneten similarity $\rightarrow$ `nbs/01_similarities.ipynb`
- cluster polls $\rightarrow$ `nbs/04_poll_clustering.ipynb`
- predict politician votes $\rightarrow$ `nbs/05_predicting_votes.ipynb`

For a short overview of the highlights see below.

## Data sources

* Bundestag page `https://www.bundestag.de/parlament/plenum/abstimmung/liste`:
    * contains: Roll call votes with information on presence / absence and vote (yes/no/abstain) for each member of the Bundestag over a longer time period
    * used in: `nbs/01_similarities.ipynb` and `nbs/02_gui.ipynb` to investigate similarities between parties and politicians based on voting behavior
* abgeordnetenwatch API `https://www.abgeordnetenwatch.de/api` (they also have a GUI [here](https://www.abgeordnetenwatch.de)):
    * contains information on politicians, parliaments, legislative periods and mandates including and beyond the Bundestag
    * used in: `nbs/04_poll_clustering.ipynb` and `nbs/05_predicting_votes.ipynb` to cluster polls by description and predict votes of individual politicians respectively

## Handling data

### Downloading raw data

Abgeordnetenwatch via cli
```shell
bundestag download abgeordnetenwatch 111
```

or via python
```python
bundestag.data.download.abgeordnetenwatch.run(111,raw_path=Path("data/raw/abgeordnetenwatch"),preprocessed_path=Path("data/preprocessed/abgeordnetenwatch"))
```

Bundestag sheets via cli
```shell
bundestag download bundestag_sheet
```

or via python
```python
bundestag.data.download.bundestag_sheets.run(html_path=Path("data/raw/bundestag/htm_files"),sheet_path=Path("data/preprocessed/bundestag/sheets"))
```

### Transforming the data

Abgeordnetenwatch via cli
```shell
bundestag transform abgeordnetenwatch 111
```

or via python
```python
bundestag.data.transform.abgeordnetenwatch.run(111,raw_path=Path("data/raw/abgeordnetenwatch"),preprocessed_path=Path("data/preprocessed/abgeordnetenwatch"))
```

Bundestag sheets via cli
```shell
bundestag transform bundestag_sheet
```

or via python
```python
bundestag.data.download.bundestag_sheets.run(html_path=Path("data/raw/bundestag/htm_files"),sheet_path=Path("data/raw/bundestag/sheets"), preprocessed_path=Path("data/preprocessed/bundestag"))
```

### Downloading raw and preprocessed data from huggingface

Via cli
```shell
bundestag download huggingface
```

or via python
```python
bundestag.data.download.huggingface.run(Path("data"))
```
