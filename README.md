# "Namentliche Abstimmungen"  in the Bundestag

> How do individual members of the federal German parliament (Bundestag) vote in "Namentliche Abstimmungen" (roll call votes)? How does the individual align with the different political parties? And how may the members vote on upcoming bills? All this here.

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/eschmidt42/bundestag/HEAD?labpath=docs%2Fanalysis-highlights.ipynb)
[![Tests](https://github.com/eschmidt42/bundestag/actions/workflows/ci.yml/badge.svg)](https://github.com/eschmidt42/bundestag/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/eschmidt42/bundestag/branch/main/graph/badge.svg?token=SIZEIVYX66)](https://codecov.io/gh/eschmidt42/bundestag)

This project was created out of curiosity and purely for entertainment.

What you can find here:

* tools do to process data from the sources mentioned below
* some analysis on the voting behavior of members of parliament
* a totally serious attempt to predict votes of members of parliament in upcoming roll call votes.

## Data sources

The German parliament makes roll call votes available as XLSX / XLS files (and PDFs ¯\\\_(ツ)\_/¯ ) here: https://www.bundestag.de/parlament/plenum/abstimmung/liste.

The NGO [abgeordnetenwatch](https://www.abgeordnetenwatch.de/) provides an [open API](https://www.abgeordnetenwatch.de/api) for a variety of related data. They also provide a great way of inspecting the voting behavior of members of parliament and their (non-)responses to question asked by the public.

## Analysis highlight - Embedded members of parliament

As a side effect of trying to predict the vote of individual members of parliament we can obtain embeddings for each member. Doing so for the 2017-2021 legislative period, we find that they cluster into governing coalition (CDU/CSU & SPD) and the opposition:
![](docs/images/mandate_embeddings.png)

![](docs/images/surprised-pikachu.png)

If you want to see more check out [this site](docs/analysis-highlights.md) or [this notebook](docs/analysis-highlights.ipynb).

## How to install

```shell
git clone https://github.com/eschmidt42/bundestag
cd bundestag
make install-dev-env
```

## How to use

### Jupyter notebooks

* [highlight notebook](docs/analysis-highlights.ipynb)
* parsing data:
    * [pt I - scraped bundestag page](nbs/00_html_parsing.ipynb)
    * [pt II - abgeordnetenwatch api](nbs/03_abgeordnetenwatch_data.ipynb)
* analysis & modelling:
    * [pt I - parlamentarian-faction and faction-faction similarities](nbs/01_similarities.ipynb)
    * [pt II - predicting votes of parlamentarians](nbs/05_predicting_votes.ipynb)


### The `bundestag` cli

A tool to assist with the data processing.

For an overview over commands run
```shell
uv run bundestag --help
```

To download data from abgeordnetenwatch, for a specific legislature id
```shell
uv run bundestag download abgeordnetenwatch 132
```

To transform the downloaded data run
```shell
uv run bundestag transform abgeordnetenwatch 132
```

To find out the legislature id for the current Bundestag, visit [abgeordnetenwatch.de](https://www.abgeordnetenwatch.de/bundestag) and click on the "Open Data" button at the bottom of the page.

To download prepared raw and transformed data from huggingface run
```shell
uv run bundestag download huggingface
```

### The `get_xlsx_uris` cli

Pre-processing cli for `bundestag` cli.

    uv run get_xlsx_uris run --help

Module for collecting and storing XLSX URIs from Bundestag data sources. Also done with

    uv run bundestag download bundestag_sheet --do_create_xlsx_uris_json
