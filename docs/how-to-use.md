## How to use

### Jupyter notebooks

* [highlight notebook](analysis-highlights.ipynb)
* parsing data:
    * pt I - scraped bundestag page: `nbs/00_html_parsing.ipynb`
    * pt II - abgeordnetenwatch api: `nbs/03_abgeordnetenwatch_data.ipynb`
* analysis & modelling:
    * pt I - parlamentarian-faction and faction-faction similarities: `nbs/01_similarities.ipynb`
    * pt II - predicting votes of parlamentarians: `nbs/05_predicting_votes.ipynb`

### The `bundestag` cli

A tool to assist with the data processing.

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
```

To find out the legislature id for the current Bundestag, visit [abgeordnetenwatch.de](https://www.abgeordnetenwatch.de/bundestag) and click on the "Open Data" button at the bottom of the page.

To download prepared raw and transformed data from huggingface run
```shell
bundestag download huggingface
```
