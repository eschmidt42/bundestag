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
