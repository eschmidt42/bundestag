import itertools
import logging
from typing import Any

import gensim
import gensim.corpora as corpora
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
import spacy
from sklearn import decomposition

logger = logging.getLogger(__name__)


def remove_stopwords_and_punctuation(
    text: str, nlp: spacy.language.Language
) -> list[str]:
    return [str(w) for w in nlp(text) if not (w.is_stop or w.is_punct)]


def remove_numeric_and_empty(text: list[str]) -> list[str]:
    return [w for w in text if not (w.isnumeric() or w.isspace())]


def make_topic_scores_dense(scores: list[list[tuple[int, Any]]]) -> np.ndarray:
    "Transforms `scores` (result of lda_model[corpus]) to a numpy array of shape (Ndoc,Ntopic)"

    topic_ids = [v for (v, _) in itertools.chain(*scores)]
    non_int_topic_ids = [v for v in topic_ids if not isinstance(v, int)]
    if len(non_int_topic_ids) > 0:
        raise ValueError(
            f"Unexpectedly received non-int topic ids: {non_int_topic_ids}"
        )

    ntopic = int(max(topic_ids))  # type: ignore
    ndoc = len(scores)

    logger.debug(
        f"Densifying list of {ntopic} topic scores to {ndoc}-by-{ntopic} array"
    )

    dense = np.zeros(shape=(ndoc, ntopic + 1))

    for i, doc in enumerate(scores):
        for topic_id, score in doc:
            dense[i, topic_id] = score
    return dense


def clean_text(s: str, nlp: spacy.language.Language) -> list[str]:
    logger.debug("Cleaning texts")
    _s = " ".join(s.split("\n")).replace("\xa0", " ")
    _texts = remove_stopwords_and_punctuation(_s, nlp)
    _texts = remove_numeric_and_empty(_texts)
    return _texts


class SpacyTransformer:
    "Performs cleaning and document to vector transformations"

    def __init__(self, language: str = "de_core_news_sm"):
        self.nlp: spacy.language.Language = spacy.load(language)

    def model_preprocessing(self, documents: list[list[str]]):
        "Basic preprocessing steps required for gensim models"
        logger.debug("Performing basic model preprocessing steps")
        self.dictionary = corpora.Dictionary(documents)
        self.corpus = [self.dictionary.doc2bow(doc) for doc in documents]

    def fit_lda(self, documents: list[list[str]], num_topics: int = 10):
        "Fits `num_topics` LDA topic models to `documents` (iterable of iterable of strings)"
        self.model_preprocessing(documents)

        logger.debug(f"Fitting LDA topics for {len(documents)}")
        self.lda_model = gensim.models.LdaMulticore(
            corpus=self.corpus, id2word=self.dictionary, num_topics=num_topics
        )
        self.lda_topics = {
            i: descr
            for (i, descr) in self.lda_model.print_topics(num_topics=num_topics)
        }

    def transform_documents(
        self, documents: pl.DataFrame, col: str, label: str = "nlp"
    ) -> pl.DataFrame:
        "Transforming `documents` to an ndoc-by-ndim matrix"
        logger.debug("Transforming `documents` to a dense real matrix")
        corpus = [self.dictionary.doc2bow(doc) for doc in documents[col]]
        scores = list(self.lda_model[corpus])
        dense = make_topic_scores_dense(scores)  # type: ignore
        return documents.with_columns(
            **{f"{label}_dim{i}": pl.Series(dense[:, i]) for i in range(dense.shape[1])}
        ).drop(col)

    def transform(
        self,
        df: pl.DataFrame,
        col: str = "poll_title_nlp_processed",
        label: str = "nlp",
        return_new_cols: bool = False,
    ):
        df = df.with_row_index(name="index")
        df_lda = self.transform_documents(df.select(["index", col]), col, label=label)
        tmp = df.join(df_lda, on="index")

        new_cols = [c for c in df_lda.columns if c.startswith(label)]
        logger.debug(f"Adding nlp features: {new_cols}")

        if return_new_cols:
            return tmp, new_cols
        return tmp


def get_word_frequencies(df: pd.DataFrame, col: str) -> pd.Series:
    "Word count over a list of lists, assuming every lowest level list item is a word."
    return pd.Series([_w for w in df[col].values for _w in w]).value_counts()


def compare_word_frequencies(
    df: pd.DataFrame,
    col0: str,
    col1: str,
    topn: int = 20,
    label0: str = "before spacy",
    label1: str = "after spacy",
    title="Word counts before and after spacy processing",
):
    "Displays top words across all texts as well as the word count distributions, comparing with and without spacy processing"
    tmp = df.copy()
    _col0 = f"{col0}_split"
    tmp[_col0] = tmp[col0].str.split(" ")

    print(
        f"Top {topn} word frequencies for {col0}: \n{get_word_frequencies(tmp, _col0).head(topn)}"
    )
    print(
        f"\nTop {topn} word frequencies for {col1}: \n{get_word_frequencies(tmp, col1).head(topn)}"
    )

    wc_col0 = f"{_col0}_word_count"
    wc_col1 = f"{col1}_word_count"
    tmp[wc_col0] = tmp[_col0].str.len()
    tmp[wc_col1] = tmp[col1].str.len()

    fig, ax = plt.subplots()
    bins = np.arange(30)
    sns.histplot(
        data=tmp,
        x=wc_col0,
        alpha=0.4,
        ax=ax,
        color="blue",
        label=label0,
        bins=bins,
    )
    sns.histplot(
        data=tmp,
        x=wc_col1,
        alpha=0.4,
        ax=ax,
        color="green",
        label=label1,
        bins=bins,
    )
    ax.legend()
    ax.set(title=title)
    plt.tight_layout()
    return ax


def preprocess_polls_for_plotting(
    df_polls: pl.DataFrame, st: SpacyTransformer, nlp_feature_cols: list[str]
) -> pl.DataFrame:
    dense = df_polls[nlp_feature_cols].to_numpy()
    pca_model = decomposition.PCA(n_components=2).fit(dense)
    X = pca_model.transform(dense)

    most_relevant_topic = dense.argmax(axis=1)
    most_relevant_topic_p = dense[range(len(dense)), most_relevant_topic]

    tmp = pl.DataFrame(
        {
            "lda_pca_component_0": pl.Series(X[:, 0]),
            "lda_pca_component_1": pl.Series(X[:, 1]),
            "most_relevant_topic_p": pl.Series(most_relevant_topic_p),
            "most_relevant_topic_ix": pl.Series(most_relevant_topic),
            "most_relevant_topic_full": pl.Series(
                [st.lda_topics[ix] for ix in most_relevant_topic]
            ),
        }
    )
    df_polls = pl.concat((df_polls, tmp), how="horizontal")
    return df_polls
