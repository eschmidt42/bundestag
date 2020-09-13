# Finding matches for MdBs
> Ever wondered, given an MdB, which are the most similar MdBs based on their voting behavior? This is what this repo does.


## How to use

### Downloading the data

The German Parliament is so friendly to put all votes of all members into readable XLSX / XLS files, which can be found here: https://www.bundestag.de/parlament/plenum/abstimmung/liste.

Let's first define the source dir with the html data and and the target dir for the downloaded XLSX / XLS files 

```python
html_data_dir = Path('../raw_data')
xlsx_data_dir = Path('../xlsx_data')
```

Now let's collect the relevant URIs from the html documents

```python
%%time
html_file_paths = parsing.get_file_paths(html_data_dir, suffix='.htm')
xlsx_uris = parsing.collect_xlsx_uris(html_file_paths)
```

With the URIs we can now download the XLSX / XLS files

```python
%%time
xlsx_file_title_maps = parsing.download_multiple_xlsx_files(xlsx_uris, xlsx_dir=xlsx_data_dir)
```

and store them in a `pd.DataFrame`

```python
xlsx_files = parsing.get_file_paths(xlsx_data_dir, pattern=re.compile('(\.xlsx?)'))
df = parsing.collect_xlsxs_as_dict(xlsx_files, xlsx_file_title_maps=xlsx_file_title_maps)
```

### Calculating Similarities between MdBs (Mitglied des Bundestages)

Before we can process the similarities / agreements between the MdBs let's reshape `df`

```python
df_squished = similarity.get_squished_dataframe(df)
```

and now for the agreements between the MdBs

```python
agreements = similarity.scan_all_agreements(df_squished)
```

With agreement between two MdBs we here use 1 - [Jaccard distance](https://en.wikipedia.org/wiki/Jaccard_index) times 100. This is the intersection of the issues pairs of MdBs have voted on in the same way divided by the total number of issues the pairs have voted on this way. So if two MdBs have voted on all the same issues and voted always the same way their agreement is 100%. 

### Running the GUI

Using just calculated `df` and `agreements`

```python
_gui = gui.GUI(df, agreements)
```

Using pre-computed `df` and `agreements`

```python
_gui = gui.GUI(gui.df, gui.agreements)
```

Running the GUI

```python
_gui.run()
```
