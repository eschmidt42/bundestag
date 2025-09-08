# Inspection roll call votes / namentliche Abstimmungen data for "Fraktionszwang"

> Get insight into ["Fraktionszwang"](https://de.wikipedia.org/wiki/Fraktionsdisziplin) using voting behavior data from the bundestag and abgeordnetenwatch.

## TL;DR

* Measured diversity of votes by party using entropy.
* All parties have their members largely voting along party line, but not deviation is the norm. The only group voting close to radically random is those of the factionless.

## Setup

Fraktionszwang should become evident by how diverse the votes are by one party across different polls.

To collect data, if not already present in `../data/preprocessed` run

    uv run bundestag download huggingface


```python
%load_ext autoreload
%autoreload 2
```


```python
import polars as pl
from plotnine import (
    ggplot,
    aes,
    geom_point,
    labs,
    scale_y_continuous,
    facet_wrap,
    theme,
    geom_line,
    scale_color_manual,
)
import os
from pathlib import Path
from bundestag.fine_logging import setup_logging
import logging
from bundestag.paths import get_paths
import math
from bundestag.data.transform.abgeordnetenwatch.transform import (
    get_polls_parquet_path,
    get_votes_parquet_path,
    get_mandates_parquet_path,
)

logger = logging.getLogger(__name__)


def plot_poll_counts_over_time(df: pl.dataframe, x: str, y: str) -> ggplot:
    return (
        ggplot(df, aes("date", "n"))
        + geom_point()
        + labs(title="# polls per day over time", x="Date", y="# unique polls")
        + scale_y_continuous(breaks=[0, 2, 4, 6, 8, 10])
    )


def plot_voting_members_per_poll_over_time(
    df: pl.DataFrame, x: str, poll: str, member: str
) -> ggplot:
    members_per_poll_per_day_over_time = df.group_by([x, poll]).agg(
        pl.col(member).n_unique().alias("n")
    )
    return (
        ggplot(members_per_poll_per_day_over_time, aes(x, "n"))
        + geom_point()
        + labs(
            title="# Members voting per poll per day over time", x="Date", y="# members"
        )
    )


def compute_vote_shares(
    df: pl.DataFrame, x: str, poll: str, party: str, vote: str, member: str
) -> pl.DataFrame:
    member_votes_per_faction_per_poll_per_day_over_time = (
        df.group_by([x, poll, party, vote])
        .agg(pl.col(member).n_unique().alias("n"))
        .sort(x, poll, party, vote)
    )
    return member_votes_per_faction_per_poll_per_day_over_time.with_columns(
        (pl.col("n") / pl.sum("n").over([x, poll, party])).alias("vote share")
    )


def plot_voting_shares_over_time(
    df: pl.DataFrame, x: str, party: str, colors: scale_color_manual
) -> ggplot:
    return (
        ggplot(
            df,
            aes(x, "vote share", color="vote"),
        )
        + geom_point(alpha=0.3)
        + labs(
            title="Voting shares per poll per day over time",
            x="Date",
            y="Vote fraction",
        )
        + facet_wrap(party, ncol=1)
        + scale_y_continuous(limits=(0, 1), breaks=[0, 0.25, 0.5, 0.75, 1.0])
        + theme(figure_size=(10, 16), subplots_adjust={"hspace": 0.35})
        + colors
    )


def get_max_entropy(df: pl.DataFrame, col: str) -> float:
    return -math.log2(1 / df[col].n_unique())


def compute_entropies(
    df: pl.DataFrame, t: str, poll: str, party: str, vote: str
) -> pl.DataFrame:
    max_entropy = get_max_entropy(df, vote)
    return (
        df.with_columns(**{"log p": pl.col("vote share").log(base=2)})
        .group_by([t, poll, party])
        .agg(
            **{
                "shannon entropy": -pl.when(pl.col("vote share") > 0)
                .then(pl.col("vote share") * pl.col("log p"))
                .otherwise(0)
                .sum()
            }
        )
        .with_columns(
            **{
                "share of max shannon entropy [%]": pl.col("shannon entropy")
                / max_entropy
            }
        )
    )


def compute_rolling_median(
    df: pl.DataFrame,
    n: int,
    x: str,
    party: str,
    y: str = "share of max shannon entropy [%]",
) -> pl.DataFrame:
    return df.sort(x).with_columns(
        pl.col(y).rolling_median(window_size=n).over(party).alias("rolling_median")
    )


def plot_entropy_over_time(
    df: pl.DataFrame,
    party_colors: scale_color_manual,
    x: str = "date",
    y: str = "share of max shannon entropy [%]",
    color: str = "party",
) -> ggplot:
    return (
        ggplot(df, aes(x, y, color=color))
        + geom_point(alpha=0.3)
        + labs(
            title="Voting entropy per poll per day over time",
            x="Date",
            y="Shannon entropy (smaller = more Fraktionszwang)",
        )
        + facet_wrap(color, ncol=1)
        + theme(figure_size=(10, 16), subplots_adjust={"hspace": 0.35})
        + party_colors
        + scale_y_continuous(labels=lambda v: [f"{x * 100:.0f}%" for x in v])
    )


def plot_rolling_entropy_over_time(
    df: pl.DataFrame,
    party_colors: scale_color_manual,
    n_polls_to_average: int,
    x: str = "date",
    y: str = "rolling_median",
    color: str = "party",
) -> ggplot:
    return (
        ggplot(df, aes(x=x, color=color))
        + geom_line(aes(y=y))
        + labs(
            title=f"Relative voting entropy per poll per day over time with rolling median (n={n_polls_to_average})",
            x="Date",
            y="Share of Shannon entropy relative to maximum (smaller = more Fraktionszwang)",
        )
        + theme(figure_size=(8, 6), subplots_adjust={"hspace": 0.35})
        + party_colors
        + scale_y_continuous(
            labels=lambda v: [f"{x * 100:.0f}%" for x in v], limits=(0, 1)
        )
    )


# if this notebook is run via `make docs` then the environment variable is set
makedocs = os.getenv("MAKEDOCS") is not None
logger.info(f"Running nb with {makedocs=}")
```


```python
setup_logging(logging.INFO)

_fig_path = Path("./images")
paths = get_paths("../data")
paths
```

## Bundestag sheet data

### Collect data


```python
file = paths.preprocessed_bundestag / "bundestag.de_votes.parquet"
file
```


```python
data_bundestag = pl.read_parquet(file)

data_bundestag.head()
```

### Clean the data

Use a single name for "Die Linke"


```python
data_bundestag["Fraktion/Gruppe"].value_counts()
```


```python
data_bundestag = data_bundestag.with_columns(
    **{
        "Fraktion/Gruppe": pl.when(pl.col("Fraktion/Gruppe").eq(pl.lit("DIE LINKE.")))
        .then(pl.lit("Die Linke"))
        .otherwise(pl.col("Fraktion/Gruppe"))
    }
)
```


```python
data_bundestag["Fraktion/Gruppe"].value_counts()
```

### Plot metrics over time

How many things are voted on per day over time?


```python
things_per_day_over_time = data_bundestag.group_by("date").agg(
    pl.col("Abstimmnr").n_unique().alias("n")
)
things_per_day_over_time.head()
```


```python
p = plot_poll_counts_over_time(things_per_day_over_time, "date", "n")
p.show()
if makedocs:
    p.save(_fig_path / "bundestag_sheets_polls_over_time.png")
```

How many members vote per poll over time?


```python
p = plot_voting_members_per_poll_over_time(
    data_bundestag, "date", "Abstimmnr", "Bezeichnung"
)
p.show()
if makedocs:
    p.save(_fig_path / "bundestag_sheets_members_per_poll_over_time.png")
```

Count of vote types by date, poll and party over time.


```python
member_votes_per_faction_per_poll_per_day_over_time = compute_vote_shares(
    data_bundestag, "date", "Abstimmnr", "Fraktion/Gruppe", "vote", "Bezeichnung"
)
```


```python
member_votes_per_faction_per_poll_per_day_over_time.head()
```


```python
colors = scale_color_manual(
    breaks=["ja", "nein", "nichtabgegeben", "Enthaltung"],
    values=["green", "red", "grey", "orange"],
)
p = plot_voting_shares_over_time(
    member_votes_per_faction_per_poll_per_day_over_time,
    "date",
    "Fraktion/Gruppe",
    colors,
)
p.show()
if makedocs:
    p.save(_fig_path / "bundestag_sheets_voting_shares_over_time.png")
```


```python
entropy_per_poll_faction = compute_entropies(
    member_votes_per_faction_per_poll_per_day_over_time,
    "date",
    "Abstimmnr",
    "Fraktion/Gruppe",
    "vote",
)
entropy_per_poll_faction.head(2)
```


```python
party_colors = scale_color_manual(
    breaks=[
        "AfD",
        "BSW",
        "BÜ90/GR",
        "CDU/CSU",
        "Die Linke",
        "FDP",
        "Fraktionslos",
        "SPD",
    ],
    values=["blue", "purple", "green", "black", "red", "yellow", "grey", "salmon"],
)
p = plot_entropy_over_time(
    entropy_per_poll_faction, party_colors, x="date", color="Fraktion/Gruppe"
)
p.show()
if makedocs:
    p.save(_fig_path / "bundestag_sheets_voting_entropy_over_time.png")
```

Now we compute the rolling median of `shannon entropy` over `n_polls_to_average` polls for each `Fraktion/Gruppe`.


```python
n_polls_to_average = 30
entropy_per_poll_faction = compute_rolling_median(
    entropy_per_poll_faction, n_polls_to_average, "date", "Fraktion/Gruppe"
)
entropy_per_poll_faction.head()
```

Now let's plot the original `shannon entropy` and the `shannon_entropy_rolling_median` to see the effect of the rolling median.


```python
p = plot_rolling_entropy_over_time(
    entropy_per_poll_faction,
    party_colors,
    n_polls_to_average,
    "date",
    color="Fraktion/Gruppe",
)
p.show()
if makedocs:
    p.save(_fig_path / "bundestag_sheets_rolling_voting_entropy_over_time.png")
```

## Abgeordnetenwatch.de data

### Collect data

First, collect data for the individual legislative periods


```python
legislature_ids = [67, 83, 97, 111, 132, 161]
```


```python
tmp = []
for legislature_id in legislature_ids:
    p = get_polls_parquet_path(legislature_id, paths.preprocessed_abgeordnetenwatch)
    if not p.exists():
        continue
    _mandates = pl.read_parquet(p)
    _mandates = _mandates.with_columns(**{"legislature_id": legislature_id})
    tmp.append(_mandates)

polls = pl.concat(tmp, how="diagonal_relaxed")
polls.head(2), polls.tail(2)
```


```python
tmp = []
for legislature_id in legislature_ids:
    p = get_votes_parquet_path(legislature_id, paths.preprocessed_abgeordnetenwatch)
    if not p.exists():
        continue
    _mandates = pl.read_parquet(p)
    _mandates = _mandates.with_columns(**{"legislature_id": legislature_id})
    tmp.append(_mandates)

votes = pl.concat(tmp, how="diagonal_relaxed")
votes.head(2), votes.tail(2)
```


```python
tmp = []
for legislature_id in legislature_ids:
    p = get_mandates_parquet_path(legislature_id, paths.preprocessed_abgeordnetenwatch)
    if not p.exists():
        continue
    _mandates = pl.read_parquet(p)
    print(len(_mandates))
    _mandates = _mandates.with_columns(**{"legislature_id": legislature_id})
    tmp.append(_mandates)

mandates = pl.concat(tmp, how="diagonal_relaxed")
mandates.head(2), mandates.tail(2)
```


```python
data_abgeordnetenwatch = polls.join(
    votes, on=["legislature_id", "poll_id"], how="left"
).join(mandates, on=["legislature_id", "mandate_id"], how="left")
```


```python
data_abgeordnetenwatch = data_abgeordnetenwatch.with_columns(
    **{"date": pl.col("poll_date").str.to_date(format="%Y-%m-%d")}
)
```


```python
with pl.Config(tbl_rows=15):
    display(data_abgeordnetenwatch["party"].value_counts(sort=True))
```

### Clean the data


```python
data_abgeordnetenwatch = data_abgeordnetenwatch.with_columns(
    **{
        "party": pl.when(
            pl.col("party").is_in(pl.lit(["DIE LINKE", "Die Linke. (Gruppe)"]))
        )
        .then(pl.lit("Die Linke"))
        .otherwise(pl.col("party"))
    }
).with_columns(
    **{
        "party": pl.when(pl.col("party").is_in(pl.lit(["DIE GRÜNEN"])))
        .then(pl.lit("BÜNDNIS 90/DIE GRÜNEN"))
        .otherwise(pl.col("party"))
    }
)
```


```python
with pl.Config(tbl_rows=15):
    display(data_abgeordnetenwatch["party"].value_counts(sort=True))
```


```python
data_abgeordnetenwatch.filter(pl.col("party").is_null()).group_by("legislature_id").agg(
    **{"mandates": pl.col("mandate_id").n_unique()}
)
```

### Plot metrics over time


```python
things_per_day_over_time = data_abgeordnetenwatch.group_by("date").agg(
    pl.col("poll_id").n_unique().alias("n")
)
things_per_day_over_time.head()
```


```python
p = plot_poll_counts_over_time(things_per_day_over_time, "date", "n")
p.show()
if makedocs:
    p.save(_fig_path / "abgeordnetenwatch_polls_over_time.png")
```


```python
p = plot_voting_members_per_poll_over_time(
    data_abgeordnetenwatch, "date", "poll_id", "mandate_id"
)
p.show()
if makedocs:
    p.save(_fig_path / "abgeordnetenwatch_members_per_poll_over_time.png")
```


```python
member_votes_per_faction_per_poll_per_day_over_time = compute_vote_shares(
    data_abgeordnetenwatch, "date", "poll_id", "party", "vote", "mandate_id"
)
```


```python
colors = scale_color_manual(
    breaks=["yes", "no", "no_show", "abstain"],
    values=["green", "red", "grey", "orange"],
)
p = plot_voting_shares_over_time(
    member_votes_per_faction_per_poll_per_day_over_time, "date", "party", colors
)
p.show()
if makedocs:
    p.save(_fig_path / "abgeordnetenwatch_voting_shares_over_time.png")
```


```python
entropy_per_poll_faction = compute_entropies(
    member_votes_per_faction_per_poll_per_day_over_time,
    "date",
    "poll_id",
    "party",
    "vote",
)
entropy_per_poll_faction.head()
```


```python
party_colors = scale_color_manual(
    breaks=[
        "AfD",
        "BSW (Gruppe)",
        "BÜNDNIS 90/DIE GRÜNEN",
        "CDU/CSU",
        "Die Linke",
        "FDP",
        "fraktionslos",
        "SPD",
    ],
    values=["blue", "purple", "green", "black", "red", "yellow", "grey", "salmon"],
)
p = plot_entropy_over_time(entropy_per_poll_faction, party_colors)
p.show()
if makedocs:
    p.save(_fig_path / "abgeordnetenwatch_voting_entropy_over_time.png")
```


```python
n_polls_to_average = 30
entropy_per_poll_faction = compute_rolling_median(
    entropy_per_poll_faction,
    n_polls_to_average,
    "date",
    "party",
    y="share of max shannon entropy [%]",
)
```


```python
p = plot_rolling_entropy_over_time(
    entropy_per_poll_faction.filter(pl.col("party").is_not_null()),
    party_colors,
    n_polls_to_average,
)
p.show()
if makedocs:
    p.save(_fig_path / "abgeordnetenwatch_rolling_voting_entropy_over_time.png")
```
