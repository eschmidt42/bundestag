from bundestag import html_parsing as hp
from bundestag import similarity as sim
from pathlib import Path
import pandas as pd
import unittest


class TestHTMLParsing(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        df = pd.read_parquet("./bundestag.de_votes.parquet")
        mdb = "Peter Altmaier"
        party_a = "SPD"
        party_b = "BÜ90/GR"
        self.party_votes = sim.get_votes_by_party(df)
        self.party_votes_pivoted = sim.pivot_party_votes_df(self.party_votes)
        self.mdb_votes = sim.prepare_votes_of_mdb(df, mdb)
        self.mdb_vs_parties = sim.align_mdb_with_parties(
            self.mdb_votes, self.party_votes_pivoted
        ).pipe(sim.compute_similarity, lsuffix="mdb", rsuffix="party")

        self.partyA_vs_partyB = sim.align_party_with_party(
            self.party_votes_pivoted, party_a=party_a, party_b=party_b
        ).pipe(sim.compute_similarity, lsuffix="a", rsuffix="b")

        self.partyA_vs_rest = sim.align_party_with_all_parties(
            self.party_votes_pivoted, party_a
        ).pipe(sim.compute_similarity, lsuffix="a", rsuffix="b")

    def test_0_votes_by_party(self):
        sim.test_party_votes(self.party_votes)

    def test_1_mdb_votes(self):
        sim.test_votes_of_mdb(self.mdb_votes)

    def test_2_mdb_vs_party(self):
        sim.test_mdb_vs_parties(self.mdb_vs_parties)

    def test_3_partyA_vs_partyB(self):
        sim.test_partyA_vs_partyB(self.partyA_vs_partyB)

    def test_4_partyA_vs_rest(self):
        sim.test_partyA_vs_partyB(self.partyA_vs_rest)
