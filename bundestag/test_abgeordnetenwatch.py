import unittest
from pathlib import Path
from bundestag import abgeordnetenwatch as aw


class TestDataCollection(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # TODO automatic retrieval of appropriate data location
        self.read_path = Path("./abgeordnetenwatch_data")
        self.write_path = Path("./abgeordnetenwatch_data_test")
        self.write_path.mkdir(exist_ok=True)
        self.legislature_id = 111
        self.poll_id = 4217
        self.num_polls = 3  # number of polls to collect if `self.dry = False`
        self.num_mandates = 4  # number of mandates to collect if `self.dry = False`
        self.dry = False

    @classmethod
    def tearDownClass(self) -> None:
        # self.write_path.rmdir()
        pass
        # return super().tearDown()

    def test_0_get_poll_info(self):
        polls = aw.get_poll_info(
            self.legislature_id, dry=self.dry, num_polls=self.num_polls
        )
        aw.store_polls_json(
            polls, self.legislature_id, dry=self.dry, path=self.write_path
        )
        if not self.dry:
            polls = aw.load_polls_json(self.legislature_id, path=self.write_path)

    def test_1_poll_data(self):
        df = aw.get_polls_df(self.legislature_id, path=self.write_path)
        aw.test_poll_data(df)

    def test_2_get_mandate_info(self):
        mandates = aw.get_mandates_info(
            self.legislature_id, dry=self.dry, num_mandates=self.num_mandates
        )
        aw.store_mandates_info(
            mandates, self.legislature_id, dry=self.dry, path=self.write_path
        )
        if not self.dry:
            mandates = aw.load_mandate_json(self.legislature_id, self.write_path)

    def test_3_mandate_data(self):
        df = aw.get_mandates_df(self.legislature_id, path=self.write_path)
        aw.test_mandate_data(df)

    def test_4_get_vote_info(self):
        votes = aw.get_vote_info(self.poll_id, dry=self.dry)
        aw.store_vote_info(votes, self.poll_id, dry=self.dry, path=self.write_path)
        if not self.dry:
            votes = aw.load_vote_json(
                self.legislature_id, self.poll_id, path=self.write_path
            )

    def test_5_vote_data(self):
        df = aw.get_votes_df(self.legislature_id, self.poll_id, path=self.write_path)
        aw.test_vote_data(df)

    def test_6_stored_vote_ids(self):
        aw.test_stored_vote_ids_check(path=self.write_path)


if __name__ == "__main__":
    unittest.main()
