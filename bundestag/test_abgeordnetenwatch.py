import unittest
from pathlib import Path
from bundestag import abgeordnetenwatch as aw


class TestDataCollection(unittest.TestCase):
    def setUp(self):
        # TODO automatic retrieval of appropriate data location
        aw.ABGEORDNETENWATCH_PATH = Path(
            "/mnt/c/PetProjects/bundestag/abgeordnetenwatch_data"
        )

    def test_poll_data(self):
        legislature_id = 111
        df = aw.get_polls_df(legislature_id)
        aw.test_poll_data(df)

    def test_mandate_data(self):
        legislature_id = 111
        df = aw.get_mandates_df(legislature_id)
        aw.test_mandate_data(df)

    def test_stored_vote_ids(self):
        aw.test_stored_vote_ids_check()

    def test_vote_data(self):
        legislature_id, poll_id = 111, 4217
        df = aw.get_votes_df(legislature_id, poll_id)
        aw.test_vote_data(df)


if __name__ == "__main__":
    unittest.main()
