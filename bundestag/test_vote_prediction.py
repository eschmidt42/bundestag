from bundestag import abgeordnetenwatch as aw
from bundestag import vote_prediction as vp
import unittest
import pandas as pd
from pathlib import Path
from fastai.tabular.all import *


class TestPredictions(unittest.TestCase):
    @classmethod
    def setUpClass(self):

        path = Path("./abgeordnetenwatch_data")

        self.df_all_votes = pd.read_parquet(path=path / "df_all_votes.parquet")

        self.df_mandates = pd.read_parquet(path=path / "df_mandates.parquet")

        y_col = "vote"

        splits = RandomSplitter(valid_pct=0.2)(self.df_all_votes)
        to = TabularPandas(
            self.df_all_votes,
            cat_names=["politician name", "poll_id"],
            y_names=[y_col],
            procs=[Categorify],
            y_block=CategoryBlock,
            splits=splits,
        )

        self.dls = to.dataloaders(bs=512)

    def test_split(self):
        vp.test_poll_split(vp.poll_splitter(self.df_all_votes))

    def test_embeddings(self):
        learn = tabular_learner(self.dls)
        learn.fit_one_cycle(1, 1e-3)
        embeddings = vp.get_embeddings(learn)
        vp.test_embeddings(embeddings)

    def test_proponents(self):
        proponents = vp.get_poll_proponents(self.df_all_votes, self.df_mandates)
        vp.test_poll_proponents(proponents)

    def test_learn_val_score(self):
        learn = tabular_learner(self.dls)
        lrs = learn.lr_find()
        learn.fit_one_cycle(5, lrs.valley)
        s = learn.validate()[0]
        thresh = 0.5
        assert s < thresh, f"Expected validation score ({s}) < {thresh}"


if __name__ == "__main__":
    unittest.main()
