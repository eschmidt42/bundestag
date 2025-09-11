# "Namentliche Abstimmungen" in the Bundestag

> How do individual members of the federal German parliament (Bundestag) vote in "Namentliche Abstimmungen" (roll call votes)? How does the individual align with the different political parties? And how may the members vote on upcoming bills? The `bundestag` cli provides tools to assist to answer those questions by providing tools download and transform the required data.

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/eschmidt42/bundestag/HEAD?labpath=docs%2Fanalysis-highlights.ipynb)
[![Tests](https://github.com/eschmidt42/bundestag/actions/workflows/ci.yml/badge.svg)](https://github.com/eschmidt42/bundestag/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/eschmidt42/bundestag/branch/main/graph/badge.svg?token=SIZEIVYX66)](https://codecov.io/gh/eschmidt42/bundestag)

## Data sources

The German parliament makes roll call votes available as XLSX / XLS files (and PDFs ¯\\\_(ツ)\_/¯ ) here: https://www.bundestag.de/parlament/plenum/abstimmung/liste.

The NGO [abgeordnetenwatch](https://www.abgeordnetenwatch.de/) provides an [open API](https://www.abgeordnetenwatch.de/api) for a variety of related data. They also provide a great way of inspecting the voting behavior of members of parliament and their (non-)responses to question asked by the public.

## Insights from the collected data

### ["Fraktionszwang"](https://de.wikipedia.org/wiki/Fraktionsdisziplin)

Do all the members of a party always follow the party line? Clearly not. But that "discipline" is similar across parties. The significant deviation are the factionless, as measured [here](https://github.com/eschmidt42/bundestag/blob/main/docs/fraktionszwang.md) using Shannon entropy. The curious mind could even estimate the energy it takes to enforce the disciplines.

![median rolling entropy over time](https://github.com/eschmidt42/bundestag/blob/main/docs/images/abgeordnetenwatch_rolling_voting_entropy_over_time.png?raw=true)

### Embedded members of parliament

As a side effect of trying to predict the vote of individual members of parliament, we can obtain embeddings for each member. Doing so for the 2017-2021 legislative period, we find that they cluster into governing coalition (CDU/CSU & SPD) and the opposition:
![2d display of mandate embeddings](https://github.com/eschmidt42/bundestag/blob/main/docs/images/mandate_embeddings.png?raw=true)

![surprised pikachu](https://github.com/eschmidt42/bundestag/blob/main/docs/images/surprised-pikachu.png?raw=true)

If you want to see more check out [this site](https://github.com/eschmidt42/bundestag/blob/main/docs/analysis-highlights.md) or [this notebook](https://github.com/eschmidt42/bundestag/blob/main/docs/analysis-highlights.ipynb).

## How to install

```shell
pip install bundestag
```

or

```shell
uv install bundestag
```

to get access to the cli to download and transform bundestag roll call votes data from bundestag.de or abgeordnetenwatch.de.

By to keep things light the machine learning dependencies are made optional. If you want to get those and related functionality as well run

```shell
pip install bundestag[ml]
```

or

```shell
uv install bundestag[ml]
```

For development

```shell
git clone https://github.com/eschmidt42/bundestag
cd bundestag
make install-dev-env
```

## How to use

### The `bundestag` cli

A tool to assist with the data processing.

For an overview over commands run
```shell
bundestag --help
```

To get preprocessed data simply run
```shell
bundestag download huggingface
```

To download data from abgeordnetenwatch, for a specific legislature id
```shell
bundestag download abgeordnetenwatch 132
```

To transform the downloaded data run
```shell
bundestag transform abgeordnetenwatch 132
```

To find out the legislature id for the current Bundestag, visit [abgeordnetenwatch.de](https://www.abgeordnetenwatch.de/bundestag) and click on the "Open Data" button at the bottom of the page.

To download data from [bundestag.de](https://www.bundestag.de/parlament/plenum/abstimmung/liste)
```shell
bundestag download bundestag-sheets --do-create-xlsx-uris-json
```

Note that this is using selenium and is therefore starting a browser. Currently this is not using a headless browser so it is easy to see when the scraping should be broken.

To transform the downloaded data run
```shell
bundestag transform bundestag-sheet --sheet-source=json_file
```

Note: If you run the extraction for the legislature with [id 132](https://www.abgeordnetenwatch.de/bundestag/20), i.e.

```shell
bundestag transform abgeordnetenwatch 132
```

the data is damaged for some reason. To fix it run

```shell
uv run python scripts/fix_empty_fraction.py
```

`download` commands will store artefacts in `./data/raw` and `transform` commands will transform that data and store artefacts in `./data/preprocessed`.

### The `get_xlsx_uris` cli

Pre-processing cli for `bundestag` cli.

```shell
uv run get_xlsx_uris run --help
```

Module for collecting and storing XLSX URIs from Bundestag data sources. Also done with

```shell
bundestag download bundestag_sheet --do-create-xlsx-uris-json
```

### Notebooks

* [fraktionszwang]((https://github.com/eschmidt42/bundestag/blob/main/docs/fraktionszwang.ipynb))
* [embedded members of parliament](https://github.com/eschmidt42/bundestag/blob/main/docs/analysis-highlights.ipynb)
