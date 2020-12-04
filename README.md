# "Namentliche Abstimmungen"  in the Bundestag
> Parse and inspect "Namentliche Abstimmungen" (roll call votes) in the Bundestag (the federal German parliament)


[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/eschmidt42/bundestag/binder0?urlpath=%2Fvoila%2Frender%2Fnbs%2F04_gui_clean.ipynb)

## Purpose of this repo

The German Parliament is so friendly to put all votes of all members into readable XLSX / XLS files (and PDFs ¯\\\_(ツ)\_/¯ ). Those files  can be found here: https://www.bundestag.de/parlament/plenum/abstimmung/liste. 

So the purpose of this repo is to help collect those roll call votes into one dataframe to enable analysis. This may be particularly interesting for the upcoming election in 2021. E.g., if you want to see what your local member of the parliament has been up to in terms of public roll call votes relative to the parties, or how individual parties agree in their votes, this dataset may be interesting for you. At this point I'd also like to point out the excellent resource [abgeordnetenwatch](https://www.abgeordnetenwatch.de/).

Since the files on the bundestag website are stored in a way making it tricky to automatically crawl them, a bit of manual work is required to generate the dataset. But don't fret! Quite a few recent roll call votes (as of the publishing of this repo) are already prepared for you. But if older or more recent roll call votes are missing, convenience tools to reduce your manual effort are demonstrated below.

An example analysis on how similar parties voted / how similar to parties individual MdBs votes, for inspiration, is also provided 😁.

## How to use

First let's look at what the processed data looks like and then how to parse it from the XLS / XLSX files.

### Inspecting the prepared data

If you have cloned the repo you should already have a `votes.parquet` file in the root directory of the repo. If not feel free to download the `votes.parquet` file directly.

```
fname = Path('../roll_call_votes.parquet')
```

```
df = pd.read_parquet(fname)
df.head()
```

### Counting how parties voted

```
party_votes, df_plot = similarity.get_votes_by_party(df)
```

```
party_votes.head()
```

```
df_plot.head()
```

### Visualizing similarities of parties over time

```
party = 'SPD'
similarity_party_party = (similarity.align_party_with_all_parties(party_votes, party)
                          .pipe(similarity.compute_similarity, lsuffix='a', rsuffix='b'))
similarity_party_party.head()
```

```
similarity.plot_similarity_over_time(similarity_party_party, 
                                     'Fraktion/Gruppe_b',
                                     title=f'{party} vs time')
```

### Running the GUI

```
g = gui.GUI(df)
```

```
# g.render()
```

### Downloading & parsing the data into a useful format

In order to collect the data and produce a dataframe like the one stored in `roll_call_votes.parquet` we need to open https://www.bundestag.de/parlament/plenum/abstimmung/liste and **manually download all the pages of interest into one location**. Then we can automatically query the html documents for the XLS / XLSX documents, download and clean those.

Let's first define the source dir with the html data and and the target dir for the downloaded XLSX / XLS files 

```
html_path = Path('../website_data')   # location where the html files were >manually< downloaded to
sheet_path = Path('../sheets') # location to automatically download the xlsx and xls files
```

Now let's download all sheet uris found in the files in `html_path` to `sheet_path`

```
nmax = 3 # number of sheets to download (set to None to download all)
df = parsing.get_multiple_sheets(html_path, sheet_path, nmax=nmax)
```

Done

```
# df.to_parquet("../new_roll_call_votes.parquet")
```
