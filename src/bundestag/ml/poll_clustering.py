import itertools
import logging
from typing import Any

import gensim
import gensim.corpora as corpora
import numpy as np
import polars as pl
import spacy
from plotnine import aes, geom_histogram, ggplot, labs, scale_fill_manual

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
    nlp_cols: list[str]

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

    def fit_lda(
        self, documents: list[list[str]], num_topics: int = 10, random_state: int = 42
    ):
        """Fits a Latent Dirichlet Allocation (LDA) model to the documents.

        This method first performs preprocessing and then fits the LDA model.

        Args:
            documents (list[list[str]]): A list of documents, where each document is a list of tokens.
            num_topics (int, optional): The number of topics for the LDA model. Defaults to 10.
        """

        self.model_preprocessing(documents)

        logger.debug(f"Fitting LDA topics for {len(documents)}")
        self.lda_model = gensim.models.LdaMulticore(
            corpus=self.corpus,
            id2word=self.dictionary,
            num_topics=num_topics,
            random_state=random_state,
        )
        self.lda_topics = {
            i: descr
            for (i, descr) in self.lda_model.print_topics(num_topics=num_topics)
        }

    def transform_documents(
        self, documents: pl.DataFrame, col: str, label: str = "topic"
    ) -> pl.DataFrame:
        """Transforms documents into a dense matrix of LDA topic scores.

        Args:
            documents (pl.DataFrame): A DataFrame containing the documents to be transformed.
            col (str): The name of the column containing the tokenized documents.
            label (str, optional): A prefix for the new topic score columns. Defaults to "topic".

        Returns:
            pl.DataFrame: A new DataFrame with columns for each topic's score.
        """

        logger.debug("Transforming `documents` to a dense real matrix")
        corpus = [self.dictionary.doc2bow(doc) for doc in documents[col]]
        scores = list(self.lda_model[corpus])
        dense = make_topic_scores_dense(scores)  # type: ignore
        self.nlp_cols = [f"{label}_{i}" for i in range(dense.shape[1])]
        return documents.with_columns(
            **{c: pl.Series(dense[:, i]) for i, c in enumerate(self.nlp_cols)}
        ).drop(col)

    def transform(
        self,
        df: pl.DataFrame,
        col: str = "poll_title_nlp_processed",
        label: str = "topic",
    ) -> pl.DataFrame:
        """Applies the fitted LDA model to a DataFrame to get topic scores.

        This method joins the topic scores back to the original DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame to transform.
            col (str, optional): The column containing the tokenized documents. Defaults to "poll_title_nlp_processed".
            label (str, optional): The prefix for the new topic score columns. Defaults to "topic".
            return_new_cols (bool, optional): If True, returns the transformed DataFrame and a list of the new column names. Defaults to False.

        Returns:
            pl.DataFrame | tuple[pl.DataFrame, list[str]]: The transformed DataFrame, or a tuple of the DataFrame and new column names.
        """
        df = df.with_row_index(name="index")
        df_lda = self.transform_documents(df.select(["index", col]), col, label=label)
        tmp = df.join(df_lda, on="index")

        new_cols = [c for c in df_lda.columns if c.startswith(label)]
        logger.debug(f"Adding nlp features: {new_cols}")

        return tmp


def get_word_frequencies(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Calculates word frequencies across a column of tokenized documents.

    Args:
        df (pl.DataFrame): A DataFrame containing the documents.
        col (str): The name of the column that holds the lists of tokens.

    Returns:
        pl.DataFrame: A DataFrame with words and their frequencies as columns, sorted in descending order.
    """

    return df[col].list.explode().value_counts(sort=True)


def compare_word_frequencies(
    df: pl.DataFrame,
    col0: str,
    col1: str,
    topn: int = 5,
) -> ggplot:
    """Compare word frequency distributions for two text columns and return a plot.

    The function prints the top `topn` most frequent tokens for both `col0` and `col1`.
    It then computes the per-document word counts for each column and returns a
    histogram plot (as a plotnine `ggplot` object) showing the distribution of
    word counts before and after processing.

    Notes:
    - `col0` is split on spaces to create token lists; `col1` is expected to already
      contain token lists (for example, the output of a spaCy-based cleaning step).
    - The function uses Polars expressions and returns a plotnine `ggplot` object.

    Args:
        df (pl.DataFrame): Polars DataFrame containing the text columns.
        col0 (str): Name of the original/raw text column (string) which will be split on spaces.
        col1 (str): Name of the processed/tokenized column (list/array of tokens).
        topn (int, optional): Number of top tokens to print for each column. Defaults to 5.

    Returns:
        plotnine.ggplot: A ggplot object containing overlapping histograms of per-document
        word counts for `col0` (before processing) and `col1` (after processing).
    """

    col0_split = f"{col0}_split"
    df = df.with_columns(**{col0_split: pl.col(col0).str.split(" ")})

    print(
        f"Top {topn} word frequencies for {col0}: \n{get_word_frequencies(df, col0_split).head(topn)}"
    )
    print(
        f"\nTop {topn} word frequencies for {col1}: \n{get_word_frequencies(df, col1).head(topn)}"
    )

    wc_col0 = f"before spacy"
    wc_col1 = f"after spacy"
    df = df.with_columns(
        **{
            wc_col0: pl.col(col0_split).list.len(),
            wc_col1: pl.col(col1).list.len(),
        }
    )

    df = df.unpivot(
        on=["before spacy", "after spacy"],
        value_name="word count",
        variable_name="processing",
    )

    colors = {"before spacy": "blue", "after spacy": "green"}
    p = (
        ggplot(df, aes(x="word count", fill="processing"))
        + geom_histogram(position="identity", alpha=0.4, binwidth=1)
        + scale_fill_manual(values=colors)
        + labs(title="Word count frequencies shift", x="word count", fill="")
    )
    return p
