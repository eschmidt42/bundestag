# "Namentliche Abstimmungen"  in the Bundestag

> How do individual members of the federal German parliament (Bundestag) vote in "Namentliche Abstimmungen" (roll call votes)? How does the individual align with the different political parties? And how may the members vote on upcoming bills? The `bundestag` cli provides tools to assist to answer those questions by providing tools download and transform the required data.


What you can find here:

* tools do to download and process from bundestag.de and abgeordnetenwatch.de
* analyses on the voting behavior of members of the Bundestag


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
