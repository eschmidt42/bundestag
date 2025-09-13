from functools import partial

import numpy as np
import polars as pl
import pytest
import spacy

from bundestag.ml.poll_clustering import (
    SpacyTransformer,
    clean_text,
    compare_word_frequencies,
    get_word_frequencies,
    make_topic_scores_dense,
    remove_numeric_and_empty,
    remove_stopwords_and_punctuation,
)


@pytest.fixture(scope="module")
def nlp():
    return spacy.load("de_core_news_sm")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("bla bla bla", ["bla", "bla", "bla"]),
        ("bla. bla bla", ["bla", "bla", "bla"]),
        (
            "bla so bla bla",
            ["bla", "bla", "bla"],
        ),
    ],
)
def test_remove_stopwords_and_punctuations(
    text: str, expected: list[str], nlp: spacy.language.Language
):
    # line to test
    result = remove_stopwords_and_punctuation(text, nlp)

    assert result == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("bla 1 bla", ["bla", "bla"]),
        ("bla.   bla bla", ["bla", "bla", "bla"]),
        (
            "bla so bla bla",
            ["bla", "bla", "bla"],
        ),
    ],
)
def test_remove_numeric_and_empty(
    text: str, expected: list[str], nlp: spacy.language.Language
):
    processed_text = remove_stopwords_and_punctuation(text, nlp)

    # line to test
    result = remove_numeric_and_empty(processed_text)

    assert result == expected


def test_make_topic_scores_dense():
    scores = [[(0, 0.1), (1, 0.2), (2, 0.7)], [(0, 0.5), (1, 0.2), (2, 0.3)]]
    expected = np.array([[0.1, 0.2, 0.7], [0.5, 0.2, 0.3]])

    # line to test
    result = make_topic_scores_dense(scores)

    np.testing.assert_array_equal(result, expected)


def test_clean_text(nlp: spacy.language.Language):
    texts = [
        "Das ist ein Test\nmit Zeilenumbrüchen.",
        "Und hier ist noch einer mit Zahlen 123 und Satzzeichen!",
        "Ein Text\xa0mit geschütztem Leerzeichen.",
    ]

    expected_results = [
        ["Test", "Zeilenumbrüchen"],
        ["Zahlen", "Satzzeichen"],
        ["Text", "geschütztem", "Leerzeichen"],
    ]

    for t, e in zip(texts, expected_results, strict=True):
        r = clean_text(t, nlp)
        assert r == e


@pytest.fixture()
def spacy_transformer() -> SpacyTransformer:
    return SpacyTransformer()


@pytest.fixture()
def df_polls(spacy_transformer: SpacyTransformer) -> pl.DataFrame:
    col = "poll_title"
    nlp_col = f"{col}_nlp_processed"
    df_polls = pl.DataFrame(
        {
            col: [
                "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
                "Fortbestand der epidemischen Lage von nationaler Tragweite",
                "Einsatz deutscher Streitkräfte zur militärischen Evakuierung aus Afghanistan",
                "Änderung im Infektions\xadschutz\xadgesetz",
                "Keine Verwendung von geschlechtergerechter Sprache",
                "Verlängerung des Bundeswehreinsatzes vor der libanesischen Küste (UNIFIL 2021/2022)",
                "Änderungen des Bundes\xadnaturschutzgesetzes",
                "Verlängerung des Bundeswehreinsatzes im Kosovo (KFOR 2021/2022)",
                "Änderung des Klimaschutzgesetzes",
                "Epidemische Lage von nationaler Tragweite bleibt weiter bestehen",
                "Unternehmerische Sorgfaltspflichten in Lieferketten",
                "Änderung des Atomgesetzes",
                "Mehr Rechte für den Verfassungsschutz",
                "Auf\xadhebung des Vermögen\xadsteuergesetzes",
                "Aufhebung des Transsexuellengesetzes und Einführung des Selbstbestimmungsgesetzes",
                "Verlängerung des Bundeswehreinsatzes in Mali (EUTM Mali 2021/2022)",
                "Verlängerung des Bundeswehreinsatzes in Mali (MINUSMA 2021/2022)",
            ]
        }
    )
    df_polls = df_polls.with_columns(
        **{
            nlp_col: pl.col(col).map_elements(
                partial(clean_text, nlp=spacy_transformer.nlp)
            )
        }
    )
    return df_polls


class TestSpacyTransformer:
    col = "poll_title"
    nlp_col = f"{col}_nlp_processed"
    num_topics = 7
    expected_nlp_cols = [f"topic_{i}" for i in range(num_topics)]

    def test_cleaned_text(
        self, df_polls: pl.DataFrame, spacy_transformer: SpacyTransformer
    ):
        "Basic sanity check on `col` which is expected to contain lists of strings"

        res = df_polls[self.col].map_elements(
            partial(clean_text, nlp=spacy_transformer.nlp)
        )

        mask = [len(v) == 0 for v in res]
        assert not any(mask)

    def test_fit(self, df_polls: pl.DataFrame, spacy_transformer: SpacyTransformer):
        spacy_transformer.fit_lda(
            df_polls[self.nlp_col].to_list(),
            num_topics=self.num_topics,
        )

        assert spacy_transformer.lda_model is not None
        assert spacy_transformer.lda_topics is not None
        assert spacy_transformer.dictionary is not None
        assert spacy_transformer.corpus is not None

    def test_transform_documents(
        self, df_polls: pl.DataFrame, spacy_transformer: SpacyTransformer
    ):
        spacy_transformer.fit_lda(
            df_polls[self.nlp_col].to_list(),
            num_topics=self.num_topics,
        )

        df_lda = spacy_transformer.transform_documents(df_polls, self.nlp_col)

        assert df_lda.shape == (df_polls.shape[0], self.num_topics + 1)
        assert all([c in df_lda.columns for c in self.expected_nlp_cols])

    @pytest.mark.parametrize("return_new_cols", [True, False])
    def test_transform(
        self,
        return_new_cols: bool,
        df_polls: pl.DataFrame,
        spacy_transformer: SpacyTransformer,
    ):
        spacy_transformer.fit_lda(
            df_polls[self.nlp_col].to_list(),
            num_topics=self.num_topics,
        )

        df_lda = spacy_transformer.transform(
            df_polls,
            col=self.nlp_col,
        )

        assert isinstance(df_lda, pl.DataFrame)
        assert df_lda.shape == (
            df_polls.shape[0],
            self.num_topics + 1 + df_polls.shape[1],
        )
        assert all([c in df_lda.columns for c in self.expected_nlp_cols])


def test_get_word_frequencies():
    df = pl.DataFrame({"c": [["a", "b", "c"], ["a", "b"], ["a"]]})

    # line to test
    res = get_word_frequencies(df, col="c")
    assert (res["c"] == pl.Series(["a", "b", "c"])).all()
    assert (res["count"] == pl.Series([3, 2, 1])).all()


@pytest.fixture(autouse=True)
def use_matplotlib_agg():
    """Force matplotlib to use the Agg backend in tests to avoid GUI popups."""
    import matplotlib

    matplotlib.use("Agg")

    # silence the non-interactive FigureCanvasAgg show warning from plt.show()
    import warnings

    warnings.filterwarnings(
        "ignore",
        message="FigureCanvasAgg is non-interactive, and thus cannot be shown",
        category=UserWarning,
    )

    yield


def test_compare_word_frequencies():
    df = pl.DataFrame(
        {"c": ["a b c", "a b", "a"], "d": [["a", "b", "c"], ["a", "b"], ["a"]]}
    )

    # line to test
    p = compare_word_frequencies(df, col0="c", col1="d")
