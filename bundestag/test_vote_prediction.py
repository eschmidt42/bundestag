from bundestag import abgeordnetenwatch as aw
from bundestag import vote_prediction as vp
import unittest
import pandas as pd
from pathlib import Path
from fastai.tabular.all import *


class TestPredictions(unittest.TestCase):
    def setUp(self):
        # TODO automatic retrieval of appropriate data location
        aw.ABGEORDNETENWATCH_PATH = Path(
            "/mnt/c/PetProjects/bundestag/abgeordnetenwatch_data"
        )
        legislature_id = 111
        all_votes_path = (
            Path("/mnt/c/PetProjects/bundestag/abgeordnetenwatch_data")
            / f"compiled_votes_legislature_{legislature_id}.csv"
        )
        df_all_votes = pd.read_csv(all_votes_path)
        df_all_votes = df_all_votes.assign(
            **{"politician name": vp.get_politician_names}
        )
        self.df_all_votes = df_all_votes

        df_mandates = aw.get_mandates_df(legislature_id)
        df_mandates["party"] = df_mandates.apply(
            vp.get_party_from_fraction_string, axis=1
        )
        self.df_mandates = df_mandates

        y_col = "vote"

        splits = RandomSplitter(valid_pct=0.2)(df_all_votes)
        to = TabularPandas(
            df_all_votes,
            cat_names=["politician name", "poll_id"],
            y_names=[y_col],
            procs=[Categorify],
            y_block=CategoryBlock,
            splits=splits,
        )

        dls = to.dataloaders(bs=512)
        learn = tabular_learner(dls)
        lrs = learn.lr_find()
        learn.fit_one_cycle(5, lrs.valley)
        self.learn = learn

    def test_split(self):
        vp.test_poll_split(vp.poll_splitter(self.df_all_votes))

    def test_embeddings(self):
        embeddings = vp.get_embeddings(self.learn)
        vp.test_embeddings(embeddings)

    def test_proponents(self):
        proponents = vp.get_poll_proponents(self.df_all_votes, self.df_mandates)
        vp.test_poll_proponents(proponents)


if __name__ == "__main__":
    unittest.main()
