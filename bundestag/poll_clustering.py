from bundestag import abgeordnetenwatch as aw

import pandas as pd
import spacy
from loguru import logger
import sys
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

import gensim.corpora as corpora
import gensim

from sklearn import decomposition

from fastcore.all import *

logger.remove()
logger.add(sys.stderr, level="INFO")
# default level for this module should be INFO


class SpacyTransformer:
    "Performs cleaning and document to vector transformations"

    def __init__(self, language: str = "de_core_news_sm"):
        self.nlp = spacy.load(language)

    @staticmethod
    def remove_stopwords_and_punctuation(text: str, nlp):
        return [str(w) for w in nlp(text) if not (w.is_stop or w.is_punct)]

    @staticmethod
    def remove_numeric_and_empty(text: str):
        return [w for w in text if not (w.isnumeric() or w == " ")]

    def clean_text(self, df: pd.DataFrame, col: str = "poll_title"):
        logger.debug("Cleaning texts")
        return list(
            df[col]
            .str.split("\n")
            .str.join(" ")
            .str.replace("\xa0", " ")
            .apply(self.remove_stopwords_and_punctuation, nlp=self.nlp)
            .apply(self.remove_numeric_and_empty)
        )

    def _model_preprocessing(self, documents):
        "Basic preprocessing steps required for gensim models"
        logger.debug("Performing basic model preprocessing steps")
        self.dictionary = corpora.Dictionary(documents)
        self.corpus = [self.dictionary.doc2bow(doc) for doc in documents]

    def _fit_lda(self, documents, num_topics: int = 10):
        "Fits `num_topics` LDA topic models to `documents` (iterable of iterable of strings)"
        self._model_preprocessing(documents)

        logger.debug(f"Fitting LDA topics for {len(documents)}")
        self.lda_model = gensim.models.LdaMulticore(
            corpus=self.corpus, id2word=self.dictionary, num_topics=num_topics
        )
        self.lda_topics = {
            i: descr
            for (i, descr) in self.lda_model.print_topics(num_topics=num_topics)
        }

    def fit(self, documents, mode="lda", **kwargs):
        assert mode in ["lda"]
        getattr(self, f"_fit_{mode}")(documents, **kwargs)

    def _make_topic_scores_dense(self, scores):
        "Transforms `scores` (result of lda_model[corpus]) to a numpy array of shape (Ndoc,Ntopic)"
        ntopic = max([v[0] for d in scores for v in d])
        ndoc = len(scores)
        logger.debug(
            f"Densifying list of {ntopic} topic scores to {ndoc}-by-{ntopic} array"
        )
        dense = np.zeros((ndoc, ntopic))
        for i, doc in enumerate(scores):
            for j, score in doc:
                dense[i, j - 1] = score
        return dense

    def transform_documents(self, documents: pd.Series, label: str = "nlp"):
        "Transforming `documents` to an ndoc-by-ndim matrix"
        logger.debug("Transforming `documents` to a dense real matrix")
        corpus = corpus = [self.dictionary.doc2bow(doc) for doc in documents]
        dense = self._make_topic_scores_dense(self.lda_model[corpus])
        return pd.DataFrame(
            dense,
            columns=[f"{label}_dim{i}" for i in range(dense.shape[1])],
            index=documents.index,
        )

    def transform(
        self,
        df: pd.DataFrame,
        col: str = "poll_title_nlp_processed",
        do_test: bool = True,
        label: str = "nlp",
        return_new_cols: bool = False,
    ):
        df_lda = self.transform_documents(df[col], label=label)
        if do_test:
            test_dense_topic_scores(df_lda.values)
        tmp = df.copy()
        new_cols = df_lda.columns.values.tolist()
        logger.debug(f"Adding nlp features: {new_cols}")
        tmp = tmp.join(df_lda)
        if return_new_cols:
            return tmp, new_cols
        return tmp


def test_cleaned_text(df: pd.DataFrame, col: str = "poll_title_nlp_processed"):
    "Basic sanity check on `col` which is expected to contain lists of strings"
    mask = df[col].str.len() == 0
    assert (
        not mask.any()
    ), f"Unexpectedly found empty entries in `{col}`: \n{df.loc[mask]}"


def get_word_frequencies(df: pd.DataFrame, col: str):
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
        data=tmp, x=wc_col0, alpha=0.4, ax=ax, color="blue", label=label0, bins=bins
    )
    sns.histplot(
        data=tmp, x=wc_col1, alpha=0.4, ax=ax, color="green", label=label1, bins=bins
    )
    ax.legend()
    ax.set(title=title)
    plt.tight_layout()
    plt.show()


def test_dense_topic_scores(dense: np.array):
    mask = dense.sum(axis=1) > 1.0
    rows = [i for i, m in enumerate(mask) if m]
    assert (
        not mask.any()
    ), f"Found {mask.sum()} documents with topic scores in total exceeding 1 in rows: {rows}\narray:\n{dense[mask]}"
    mask = dense.sum(axis=1) <= 1e-5
    rows = [i for i, m in enumerate(mask) if m]
    assert (
        not mask.any()
    ), f"Found {mask.sum()} documents with topic scores in total smaller than 1e-5 in rows: {rows}\narray:\n{dense[mask]}"


def pca_plot_lda_topics(
    df_polls: pd.DataFrame,
    st: SpacyTransformer,
    original_text_col: str,
    nlp_feature_cols: list,
):
    "Visualises `nlp_feature_cols` columns in `df_polls` by performing a PCA and reducing to 2 dimensions."

    dense = df_polls[nlp_feature_cols].values
    pca_model = decomposition.PCA(n_components=2).fit(dense)
    X = pca_model.transform(dense)

    most_relevant_topic = dense.argmax(axis=1)
    most_relevant_topic_p = dense[range(len(dense)), most_relevant_topic]

    df_polls = df_polls.drop(
        columns=["lda_pca_component_0", "lda_pca_component_1"], errors="ignore"
    )
    tmp = pd.DataFrame(
        X, columns=["lda_pca_component_0", "lda_pca_component_1"]
    ).assign(
        **{
            "most_relevant_topic_p": most_relevant_topic_p,
            "most_relevant_topic_ix": most_relevant_topic,
            "most_relevant_topic_full": [
                st.lda_topics[ix] for ix in most_relevant_topic
            ],
        }
    )
    df_polls = df_polls.join(tmp)

    fig = px.scatter(
        data_frame=df_polls,
        x="lda_pca_component_0",
        y="lda_pca_component_1",
        hover_data=[original_text_col, "most_relevant_topic_p"],
        color="most_relevant_topic_full",
        title=f"{original_text_col}-based LDA Model visualised",
    )
    fig.update_layout(legend=dict(y=-1, x=0.01), height=700, width=1400)
    return fig
