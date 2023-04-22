import typing as T

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest
import spacy

import bundestag.poll_clustering as pc


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
    text: str, expected: T.List[str], nlp: spacy.language.Language
):
    # line to test
    result = pc.remove_stopwords_and_punctuation(text, nlp)

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
    text: str, expected: T.List[str], nlp: spacy.language.Language
):
    text = pc.remove_stopwords_and_punctuation(text, nlp)

    # line to test
    result = pc.remove_numeric_and_empty(text)

    assert result == expected


def test_make_topic_scores_dense():
    scores = [[(1, 0.1), (2, 0.2), (3, 0.7)], [(1, 0.5), (2, 0.2), (3, 0.3)]]
    expected = np.array([[0.1, 0.2, 0.7], [0.5, 0.2, 0.3]])

    # line to test
    result = pc.make_topic_scores_dense(scores)

    np.testing.assert_array_equal(result, expected)


class TestSpacyTransformer:
    col = "poll_title"
    nlp_col = f"{col}_nlp_processed"
    num_topics = 7
    expected_nlp_cols = [f"nlp_dim{i}" for i in range(num_topics - 1)]
    df_polls = pd.DataFrame(
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
    st = pc.SpacyTransformer()
    df_polls[nlp_col] = df_polls.pipe(pc.clean_text, col=col, nlp=st.nlp)

    def test_cleaned_text(self):
        "Basic sanity check on `col` which is expected to contain lists of strings"

        # line to test
        res = self.df_polls.pipe(pc.clean_text, col=self.col, nlp=self.st.nlp)

        mask = [len(v) == 0 for v in res]
        assert not any(mask)

    def test_fit(self):
        # line to test
        self.st.fit(
            self.df_polls[self.nlp_col].values,
            mode="lda",
            num_topics=self.num_topics,
        )

        assert self.st.lda_model is not None
        assert self.st.lda_topics is not None
        assert self.st.dictionary is not None
        assert self.st.corpus is not None

    def test_transform_documents(self):
        self.st.fit(
            self.df_polls[self.nlp_col].values,
            mode="lda",
            num_topics=self.num_topics,
        )

        # line to test
        df_lda = self.st.transform_documents(self.df_polls[self.nlp_col])

        assert df_lda.shape == (self.df_polls.shape[0], self.num_topics - 1)
        assert all([c in df_lda.columns for c in self.expected_nlp_cols])

    @pytest.mark.parametrize("return_new_cols", [True, False])
    def test_transform(self, return_new_cols: bool):
        self.st.fit(
            self.df_polls[self.nlp_col].values,
            mode="lda",
            num_topics=self.num_topics,
        )

        if not return_new_cols:
            # line to test
            df_lda = self.st.transform(
                self.df_polls,
                col=self.nlp_col,
                return_new_cols=return_new_cols,
            )
        else:
            # line to test
            df_lda, new_cols = self.st.transform(
                self.df_polls,
                col=self.nlp_col,
                return_new_cols=return_new_cols,
            )

        assert df_lda.shape == (
            self.df_polls.shape[0],
            self.num_topics - 1 + self.df_polls.shape[1],
        )
        assert all([c in df_lda.columns for c in self.expected_nlp_cols])
        if return_new_cols:
            assert all([c in new_cols for c in self.expected_nlp_cols])

    def test_pca_plot_lda_topics(self):
        self.st.fit(
            self.df_polls[self.nlp_col].values,
            mode="lda",
            num_topics=self.num_topics,
        )
        df_lda, nlp_feature_cols = self.st.transform(
            self.df_polls, col=self.nlp_col, return_new_cols=True
        )

        # line to test
        ax = pc.pca_plot_lda_topics(
            df_lda, self.st, self.col, nlp_feature_cols
        )

        assert isinstance(ax, go.Figure)


def test_get_word_frequencies():
    df = pd.DataFrame({"c": [["a", "b", "c"], ["a", "b"], ["a"]]})

    # line to test
    res = pc.get_word_frequencies(df, col="c")

    assert res["a"] == 3
    assert res["b"] == 2
    assert res["c"] == 1


def test_compare_word_frequencies():
    df = pd.DataFrame(
        {"c": ["a b c", "a b", "a"], "d": [["a", "b", "c"], ["a", "b"], ["a"]]}
    )

    # line to test
    ax = pc.compare_word_frequencies(df, col0="c", col1="d")

    assert isinstance(ax, plt.Axes)
