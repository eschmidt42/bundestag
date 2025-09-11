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
    """Removes stopwords and punctuation from a text using a spaCy model.

    Args:
        text (str): The input string to be cleaned.
        nlp (spacy.language.Language): The spaCy language model to use for tokenization and identifying stopwords/punctuation.

    Returns:
        list[str]: A list of tokens with stopwords and punctuation removed.
    """
    return [str(w) for w in nlp(text) if not (w.is_stop or w.is_punct)]


def remove_numeric_and_empty(text: list[str]) -> list[str]:
    """Removes numeric and empty/whitespace-only strings from a list of strings.

    Args:
        text (list[str]): A list of tokens.

    Returns:
        list[str]: The list of tokens with numeric and empty strings removed.
    """
    return [w for w in text if not (w.isnumeric() or w.isspace())]


def make_topic_scores_dense(scores: list[list[tuple[int, Any]]]) -> np.ndarray:
    """Transforms a sparse list of topic scores into a dense numpy array.

    The input is typically the output of a gensim LDA model, which is a list of lists of (topic_id, score) tuples.
    This function converts it into a 2D numpy array of shape (num_documents, num_topics).

    Args:
        scores (list[list[tuple[int, Any]]]): A list of documents, where each document is a list of (topic_id, score) tuples.

    Raises:
        ValueError: If any topic ID is not an integer.

    Returns:
        np.ndarray: A dense numpy array of topic scores.
    """

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
    """Performs a series of cleaning steps on a raw text string.

    The cleaning process includes:
    1. Replacing newlines and non-breaking spaces.
    2. Removing stopwords and punctuation using a spaCy model.
    3. Removing numeric and empty/whitespace-only tokens.

    Args:
        s (str): The raw input string.
        nlp (spacy.language.Language): The spaCy language model to use for cleaning.

    Returns:
        list[str]: A list of cleaned tokens.
    """
    logger.debug("Cleaning texts")
    _s = " ".join(s.split("\n")).replace("\xa0", " ")
    _texts = remove_stopwords_and_punctuation(_s, nlp)
    _texts = remove_numeric_and_empty(_texts)
    return _texts


class SpacyTransformer:
    """A transformer class for cleaning text and fitting/applying a gensim LDA model."""

    def __init__(self, language: str = "de_core_news_sm"):
        """Initializes the transformer by loading a spaCy language model.

        Args:
            language (str, optional): The name of the spaCy language model to load. Defaults to "de_core_news_sm".
        """
        self.nlp: spacy.language.Language = spacy.load(language)

    def model_preprocessing(self, documents: list[list[str]]):
        """Performs basic preprocessing required for gensim models.

        This method creates a gensim dictionary and a bag-of-words corpus from the documents.

        Args:
            documents (list[list[str]]): A list of documents, where each document is a list of tokens.
        """

        logger.debug("Performing basic model preprocessing steps")
        self.dictionary = corpora.Dictionary(documents)
        self.corpus = [self.dictionary.doc2bow(doc) for doc in documents]

    def fit_lda(self, documents: list[list[str]], num_topics: int = 10):
        """Fits a Latent Dirichlet Allocation (LDA) model to the documents.

        This method first performs preprocessing and then fits the LDA model.

        Args:
            documents (list[list[str]]): A list of documents, where each document is a list of tokens.
            num_topics (int, optional): The number of topics for the LDA model. Defaults to 10.
        """

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
        """Transforms documents into a dense matrix of LDA topic scores.

        Args:
            documents (pl.DataFrame): A DataFrame containing the documents to be transformed.
            col (str): The name of the column containing the tokenized documents.
            label (str, optional): A prefix for the new topic score columns. Defaults to "nlp".

        Returns:
            pl.DataFrame: A new DataFrame with columns for each topic's score.
        """

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
        """Applies the fitted LDA model to a DataFrame to get topic scores.

        This method joins the topic scores back to the original DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame to transform.
            col (str, optional): The column containing the tokenized documents. Defaults to "poll_title_nlp_processed".
            label (str, optional): The prefix for the new topic score columns. Defaults to "nlp".
            return_new_cols (bool, optional): If True, returns the transformed DataFrame and a list of the new column names. Defaults to False.

        Returns:
            pl.DataFrame | tuple[pl.DataFrame, list[str]]: The transformed DataFrame, or a tuple of the DataFrame and new column names.
        """
        df = df.with_row_index(name="index")
        df_lda = self.transform_documents(df.select(["index", col]), col, label=label)
        tmp = df.join(df_lda, on="index")

        new_cols = [c for c in df_lda.columns if c.startswith(label)]
        logger.debug(f"Adding nlp features: {new_cols}")

        if return_new_cols:
            return tmp, new_cols
        return tmp


def get_word_frequencies(df: pd.DataFrame, col: str) -> pd.Series:
    """Calculates word frequencies across a column of tokenized documents.

    Args:
        df (pd.DataFrame): A DataFrame containing the documents.
        col (str): The name of the column that holds the lists of tokens.

    Returns:
        pd.Series: A Series with words as the index and their frequencies as values, sorted in descending order.
    """

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
    """Compares and visualizes word frequencies and counts before and after text processing.

    This function prints the top N most frequent words for two different text columns
    (e.g., raw text and cleaned text) and plots histograms of the word counts per document
    for both columns.

    Args:
        df (pd.DataFrame): The DataFrame containing the text columns.
        col0 (str): The name of the first column (e.g., raw text).
        col1 (str): The name of the second column (e.g., cleaned text).
        topn (int, optional): The number of top words to print. Defaults to 20.
        label0 (str, optional): The label for the first column in the plot. Defaults to "before spacy".
        label1 (str, optional): The label for the second column in the plot. Defaults to "after spacy".
        title (str, optional): The title of the plot. Defaults to "Word counts before and after spacy processing".

    Returns:
        matplotlib.axes.Axes: The Axes object of the generated plot.
    """

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
    """Preprocesses poll data for visualization by reducing dimensionality and identifying key topics.

    This function performs the following steps:
    1. Applies PCA to reduce the LDA topic score features to 2 dimensions.
    2. Identifies the most relevant topic for each poll based on the highest score.
    3. Adds the PCA components and topic information as new columns to the DataFrame.

    Args:
        df_polls (pl.DataFrame): The DataFrame containing poll data with LDA topic scores.
        st (SpacyTransformer): The fitted SpacyTransformer object, used to access topic descriptions.
        nlp_feature_cols (list[str]): A list of column names corresponding to the LDA topic scores.

    Returns:
        pl.DataFrame: The enhanced DataFrame with new columns for PCA components and topic analysis, ready for plotting.
    """
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
