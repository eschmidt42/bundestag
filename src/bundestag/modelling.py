import re
import typing as T

import numpy as np
import pandas as pd
import spacy
from fastai.tabular.all import (
    Categorify,
    CategoryBlock,
    Normalize,
    TabularPandas,
    tabular_learner,
)
from gensim import corpora, matutils
from gensim.models import TfidfModel
from sklearn import base, feature_extraction

import bundestag.poll_clustering as pc
import bundestag.vote_prediction as vp


class PartyInGovernmentTransformer(base.BaseEstimator, base.TransformerMixin):
    def __init__(
        self,
        party_col: str = "party",
        parties: T.List[str] = [
            "CDU/CSU",
            "SPD",
            "AfD",
            "FDP",
            "DIE LINKE",
            "DIE GRÜNEN",
            "fraktionslos",
        ],
    ):
        self.party_col = party_col
        self.parties = parties

    def fit(self, X: pd.DataFrame, y=None):
        # legislature_period: [party_a, party_b, ...]
        self.governing_parties = {
            "Bundestag 2013 - 2017": ["CDU/CSU", "SPD"],
            "Bundestag 2017 - 2021": ["CDU/CSU", "SPD"],
            "Bundestag 2021 - 2025": ["SPD", "FDP", "DIE GRÜNEN"],
        }

        return self

    def _is_in_government(self, row: pd.Series):
        return (
            row[self.party_col]
            in self.governing_parties[row["legislature_period"]]
        )

    def transform(self, X):
        _X = X.copy()
        _X["party_is_in_government"] = _X.apply(self._is_in_government, axis=1)
        return _X


class BillProposedByGovernmentTransformer(
    base.BaseEstimator, base.TransformerMixin
):
    def __init__(self, description_col: str = "poll_description"):
        self.description_col = description_col

    def fit(self, X: pd.DataFrame, y=None):
        # pattern that captures if Bundesregierung or Regierungskoalition appear in the first sentence
        self.patt_ = re.compile(
            r"^(?:(?!\.).)*\s(Bundesregierung|Regierungskoalition)\s.*\."
        )
        return self

    def _bill_is_proposed_by_government(self, row: pd.Series):
        # regex for "Bundesregierung" or "Regierungskoalition" in row[self.description_col]
        # if found, return True, else False

        return self.patt_.match(row[self.description_col]) is not None

    def transform(self, X):
        _X = X.copy()
        _X["bill_proposed_by_government"] = _X.apply(
            self._bill_is_proposed_by_government, axis=1
        )
        return _X


def detect_columns(
    X: pd.DataFrame, beginnings: T.List[str], endings: T.List[str]
):
    cols = [
        c
        for c in X.columns
        if any([c.startswith(b) for b in beginnings])
        or any([c.endswith(e) for e in endings])
    ]
    return cols


class ColumnRemoverTransformer(base.BaseEstimator, base.TransformerMixin):
    def __init__(
        self,
        columns_to_remove: T.List[str] = [],
        column_beginnings: T.List = [],
        column_endings: T.List = [],
    ):
        self.columns_to_remove = columns_to_remove
        self.column_beginnings = column_beginnings
        self.column_endings = column_endings

    def fit(self, X: pd.DataFrame, y=None):
        self.detected_columns_to_remove_ = detect_columns(
            X, self.column_beginnings, self.column_endings
        )
        return self

    def transform(self, X):
        _X = X.copy()
        _X = _X.drop(
            columns=self.columns_to_remove + self.detected_columns_to_remove_
        )
        return _X


class FastAIClassifier(base.BaseEstimator, base.ClassifierMixin):
    def __init__(
        self,
        cat_names_beginnings: T.List[str] = [],
        cont_names_beginnings: T.List[str] = [],
        cat_names_endings: T.List[str] = [
            "mandate_id",
            "party_is_in_government",
            "party",
            "bill_proposed_by_government",
            "legislature_period",
        ],
        cont_names_endings: T.List[str] = [],
        poll_id_col: str = "poll_id",
        batch_size: int = 1024,
        n_epochs: int = 5,
    ) -> None:
        self.cat_names_beginnings = cat_names_beginnings
        self.cont_names_beginnings = cont_names_beginnings
        self.cat_names_endings = cat_names_endings
        self.cont_names_endings = cont_names_endings
        self.poll_id_col = poll_id_col
        self.batch_size = batch_size
        self.n_epochs = n_epochs

    def _detect_columns(
        self, X: pd.DataFrame, beginnings: T.List[str], endings: T.List[str]
    ):
        cols = [
            c
            for c in X.columns
            if any([c.startswith(b) for b in beginnings])
            or any([c.endswith(e) for e in endings])
        ]
        return cols

    def fit(self, X: pd.DataFrame, y: pd.Series):
        X = X.copy(deep=True)
        y = y.copy(deep=True)

        self.classes_ = y.unique()

        Xy = X.assign(vote=y)
        Xy.index = range(len(Xy))
        splits = vp.poll_splitter(Xy, valid_pct=0.2, poll_col=self.poll_id_col)

        self.cat_cols_ = self._detect_columns(
            Xy, self.cat_names_beginnings, self.cat_names_endings
        )
        self.cont_cols_ = self._detect_columns(
            Xy, self.cont_names_beginnings, self.cont_names_endings
        )

        intersecting_cols = set(self.cat_cols_).intersection(
            set(self.cont_cols_)
        )
        if intersecting_cols != set():
            raise ValueError(
                f"Columns {intersecting_cols} are both in cat_cols and cont_cols. Please adjust what you passed in *names_beginnings and or *names_endings."
            )

        to = TabularPandas(
            Xy,
            cont_names=self.cont_cols_,
            cat_names=self.cat_cols_,
            y_names=["vote"],
            procs=[Categorify],
            y_block=CategoryBlock,
            splits=splits,
        )

        dls = to.dataloaders(bs=self.batch_size)

        self.learn_ = tabular_learner(dls)
        lrs = self.learn_.lr_find()

        self.learn_.fit_one_cycle(self.n_epochs, lrs.valley)

        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        X = X.copy(deep=True)
        y_prob, _ = self.learn_.get_preds(dl=self.learn_.dls.test_dl(X))
        return y_prob.detach().numpy()

    def predict(self, X: pd.DataFrame):
        X = X.copy(deep=True)
        _, _, idx = self.learn_.get_preds(
            dl=self.learn_.dls.test_dl(X), with_decoded=True
        )
        idx = idx.detach().numpy()
        return list(self.learn_.dls.vocab[idx])


class DenseTfidfVectorizer(base.BaseEstimator, base.TransformerMixin):
    def __init__(
        self,
        text_col: str = "poll_title",
        poll_id_col: str = "poll_id",
        language: str = "de_core_news_sm",
        no_below: int = 2,
        no_above: float = 0.5,
        keep_n: int = 100,
    ) -> None:
        self.text_col = text_col
        self.poll_id_col = poll_id_col
        self.language = language
        self.no_below = no_below
        self.no_above = no_above
        self.keep_n = keep_n
        # tfidf init code here

    def _process_text(
        self, X: pd.DataFrame, is_training: bool
    ) -> pd.DataFrame:
        polls = X.loc[:, [self.poll_id_col, self.text_col]].drop_duplicates()

        documents = pc.clean_text(polls, self.spacy_language_, self.text_col)

        if is_training:
            self.dictionary_ = corpora.Dictionary(documents)
            self.dictionary_.filter_extremes(
                no_below=self.no_below,
                no_above=self.no_above,
                keep_n=self.keep_n,
            )
            corpus = [self.dictionary_.doc2bow(doc) for doc in documents]
            self.tfidf_model_ = TfidfModel(corpus, dictionary=self.dictionary_)
        else:
            corpus = [self.dictionary_.doc2bow(doc) for doc in documents]

        tfidf_text = matutils.corpus2dense(
            self.tfidf_model_[corpus], num_terms=len(self.dictionary_)
        ).T
        columns = [
            f"tfidf_{self.text_col}_{i}" for i in range(tfidf_text.shape[1])
        ]
        tfidf_text = pd.DataFrame(tfidf_text, columns=columns, dtype=float)

        tfidf_text.index = pd.Series(
            polls[self.poll_id_col].values, name=self.poll_id_col
        )
        return tfidf_text

    def fit(self, X: pd.DataFrame, y=None):
        self.spacy_language_ = spacy.load(self.language)

        _X = X.copy(deep=True)
        _ = self._process_text(_X, is_training=True)

        # _X = _X.merge(tfidf_text, on="poll_id", how="left")

        return self

    def transform(self, X: pd.DataFrame, y=None) -> np.ndarray:
        n0 = len(X)
        ix0 = X.index.copy(deep=True)
        _X = X.copy(deep=True)

        tfidf_text = self._process_text(_X, is_training=False)

        _X = _X.join(tfidf_text, on="poll_id", how="left")
        ix1 = _X.index.copy(deep=True)
        n1 = len(_X)
        if n0 != n1:
            raise ValueError(f"Number of rows changed from {n0} to {n1}.")
        if not ix0.equals(ix1):
            raise ValueError(f"Index changed during transform. {ix0} != {ix1}")

        return _X
