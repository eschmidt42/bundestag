import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

from bundestag import abgeordnetenwatch as aw


def test_get_location():
    fname = "wup.csv"
    path = Path("file/location")

    file = aw.get_location(fname=fname, path=path, dry=True, mkdir=False)
    assert file == path / fname


@pytest.mark.parametrize(
    "dry,status_code",
    [
        (True, 200),
        (False, 200),
        (True, 201),
        (False, 201),
    ],
)
def test_get_poll_info(dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with patch(
        "requests.get", MagicMock(return_value=r)
    ) as _get, patch.object(r, "json", MagicMock()):
        # line to test
        try:
            aw.get_poll_info(42, dry=dry)
        except AssertionError as ex:
            if status_code != 200:
                pytest.xfail(
                    "Not 200 status_code value should raise an exception"
                )
            else:
                raise ex

        if dry:
            assert _get.call_count == 0
        else:
            assert _get.call_count == 1


def test_polls_file():
    assert aw.polls_file(42) == "polls_legislature_42.json"


@pytest.mark.parametrize("dry", [True, False])
def test_store_polls_json(dry: bool):
    polls = {}
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
    ):
        # line to test
        aw.store_polls_json(polls, legislature_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _mkdir.call_count == 0
            assert _open.call_count == 1
            assert _open.return_value.__enter__().write.call_count == 1


@pytest.mark.parametrize("dry", [True, False])
def test_load_polls_json(dry: bool):
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.load", MagicMock()) as json_load,
    ):
        # line to test
        aw.load_polls_json(legislature_id, path, dry=dry)

        assert json_load.call_count == 1
        assert _open.call_count == 1
        assert _mkdir.call_count == 0


POLL_DATA_RAW = {
    "id": 1240,
    "entity_type": "node",
    "label": "Verl\u00e4ngerung des Bundeswehreinsatzes in Mali (MINUSMA 2017/2018)",
    "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/1240",
    "field_legislature": {
        "id": 111,
        "entity_type": "parliament_period",
        "label": "Bundestag 2017 - 2021",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliament-periods/111",
        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19",
    },
    "field_topics": [
        {
            "id": 21,
            "entity_type": "taxonomy_term",
            "label": "Au\u00dfenpolitik und internationale Beziehungen",
            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/21",
            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/aussenpolitik-und-internationale-beziehungen",
        },
        {
            "id": 13,
            "entity_type": "taxonomy_term",
            "label": "Verteidigung",
            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/13",
            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/verteidigung",
        },
    ],
    "field_committees": None,
    "field_intro": "<p>\r\n\tDer Bundestag hat mit den Stimmen von CDU/CSU, FDP, Gr\u00fcne und SPD f\u00fcr die Einsatzverl\u00e4ngerung der Bundeswehr der NATO-Mission MINUSMA gestimmt.\r\n</p>\r\n",
    "field_poll_date": "2017-12-12",
    "field_related_links": None,
}

POLL_DATA_PARSED = {
    "poll_id": 1240,
    "poll_title": "Verlängerung des Bundeswehreinsatzes in Mali (MINUSMA 2017/2018)",
    "poll_first_committee": None,
    "poll_description": "Der Bundestag hat mit den Stimmen von CDU/CSU, FDP, Grüne und SPD für die Einsatzverlängerung der Bundeswehr der NATO-Mission MINUSMA gestimmt.",
    "legislature_id": 111,
    "legislature_period": "Bundestag 2017 - 2021",
    "poll_date": "2017-12-12",
}


def test_parse_poll_data():
    res = aw.parse_poll_data(POLL_DATA_RAW)

    assert set(list(POLL_DATA_PARSED.keys())) == set(list(res.keys()))
    for k in POLL_DATA_PARSED.keys():
        assert POLL_DATA_PARSED[k] == res[k]


def test_get_polls_df():
    info = {
        "data": [
            {
                "id": 42,
                "label": "wup",
                "field_committees": [{"label": "wooop"}],
                "field_intro": "bla",
                "field_legislature": {"id": 21, "label": "bam"},
                "field_poll_date": "2020-02-02",
            }
        ]
    }
    d = pd.DataFrame(
        [
            {
                "poll_id": 42,
                "poll_title": "wup",
                "poll_first_committee": "wooop",
                "poll_description": "bla",
                "legislature_id": 21,
                "legislature_period": "bam",
                "poll_date": "2020-02-02",
            }
        ]
    )
    with patch(
        "bundestag.abgeordnetenwatch.load_polls_json",
        MagicMock(return_value=info),
    ):
        res = aw.get_polls_df(42, "dummy/path")
        assert res.equals(d)


# def test_poll_data(df: pd.DataFrame):
#     "Basic sanity check on poll data"

#     # there should be no missing values except for poll_first_committee
#     for c in df.columns:
#         msg = f"{c}: failed because NaNs/None values were found."
#         mask = df[c].isna()
#         if c == "poll_first_committee":
#             continue
#         assert mask.sum() == 0, f"{msg}: \n{df.loc[mask].head()}"

#     # there should be no duplicated poll_id values
#     mask = df["poll_id"].duplicated()
#     assert (
#         mask.sum() == 0
#     ), f'Surprisingly found duplicated poll_id values: {df.loc[mask,"poll_id"].unique()} \nexamples: \n{df.loc[mask].head()}'


@pytest.mark.parametrize(
    "dry,status_code",
    [
        (True, 200),
        (False, 200),
        (True, 201),
        (False, 201),
    ],
)
def test_get_mandates_info(dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with patch(
        "requests.get", MagicMock(return_value=r)
    ) as _get, patch.object(r, "json", MagicMock()):
        # line to test
        try:
            aw.get_poll_info(42, dry=dry)
        except AssertionError as ex:
            if status_code != 200:
                pytest.xfail(
                    "Not 200 status_code value should raise an exception"
                )
            else:
                raise ex

        if dry:
            assert _get.call_count == 0
        else:
            assert _get.call_count == 1


def test_mandates_file():
    assert aw.mandates_file(42) == "mandates_legislature_42.json"


@pytest.mark.parametrize("dry", [True, False])
def test_store_mandates_info(dry: bool):
    polls = {}
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_mandates_info(polls, legislature_id, dry=dry, path=path)

        assert _mkdir.call_count == 0
        if dry:
            assert _open.call_count == 0
        else:
            assert _open.call_count == 1
            json_dump.assert_called_once()


@pytest.mark.parametrize("dry", [True, False])
def test_load_mandate_json(dry: bool):
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.load", MagicMock()) as json_load,
    ):
        # line to test
        aw.load_mandate_json(legislature_id, path, dry=dry)

        assert json_load.call_count == 1
        assert _open.call_count == 1
        assert _mkdir.call_count == 0


MANDATE_DATA_RAW = {
    "id": 37798,
    "entity_type": "candidacy_mandate",
    "label": "Alexander Graf Lambsdorff (Bundestag 2017 - 2021)",
    "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/37798",
    "id_external_administration": "2158",
    "id_external_administration_description": "mdbID - ID der Bundestagsverwaltung",
    "type": "mandate",
    "parliament_period": {
        "id": 111,
        "entity_type": "parliament_period",
        "label": "Bundestag 2017 - 2021",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliament-periods/111",
        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19",
    },
    "politician": {
        "id": 28887,
        "entity_type": "politician",
        "label": "Alexander Graf Lambsdorff",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/politicians/28887",
        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/profile/alexander-graf-lambsdorff",
    },
    "start_date": None,
    "end_date": None,
    "info": None,
    "electoral_data": {
        "id": 37798,
        "entity_type": "electoral_data",
        "label": "Alexander Graf Lambsdorff (Bundestag 2017 - 2021)",
        "electoral_list": {
            "id": 115,
            "entity_type": "electoral_list",
            "label": "Landesliste Nordrhein-Westfalen (Bundestag 2017 - 2021)",
            "api_url": "https://www.abgeordnetenwatch.de/api/v2/electoral-lists/115",
        },
        "list_position": 3,
        "constituency": {
            "id": 4220,
            "entity_type": "constituency",
            "label": "96 - Bonn (Bundestag 2017 - 2021)",
            "api_url": "https://www.abgeordnetenwatch.de/api/v2/constituencies/4220",
        },
        "constituency_result": 10.5,
        "constituency_result_count": None,
        "mandate_won": "list",
    },
    "fraction_membership": [
        {
            "id": 16,
            "entity_type": "fraction_membership",
            "label": "FDP",
            "fraction": {
                "id": 14,
                "entity_type": "fraction",
                "label": "FDP (Bundestag 2017 - 2021)",
                "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/14",
            },
            "valid_from": None,
            "valid_until": None,
        }
    ],
}

MANDATE_DATA_PARSED = {
    "legislature_id": 111,
    "legislature_period": "Bundestag 2017 - 2021",
    "mandate_id": 37798,
    "mandate": "Alexander Graf Lambsdorff (Bundestag 2017 - 2021)",
    "politician_id": 28887,
    "politician": "Alexander Graf Lambsdorff",
    "politician_url": "https://www.abgeordnetenwatch.de/profile/alexander-graf-lambsdorff",
    "start_date": None,
    "end_date": None,
    "constituency_id": 4220,
    "constituency_name": "96 - Bonn (Bundestag 2017 - 2021)",
    "fraction_names": ["FDP"],
    "fraction_ids": [16],
    "fraction_starts": [None],
    "fraction_ends": [None],
}


def test_parse_mandate_data():
    res = aw.parse_mandate_data(MANDATE_DATA_RAW)

    assert all([k in res for k in MANDATE_DATA_PARSED])
    for k in MANDATE_DATA_PARSED.keys():
        assert MANDATE_DATA_PARSED[k] == res[k]


# def test_mandate_data(df: pd.DataFrame):
#     "Basic sanity check on mandate data"

#     # there should be no missing values for any column in `cols`
#     cols = ["mandate_id", "mandate", "politician_id", "politician"]
#     for c in cols:
#         msg = f"{c}: failed because NaNs/None values were found."
#         mask = df[c].isna()
#         assert mask.sum() == 0, f"{msg}: \n{df.loc[mask].head()}"

#     # there should only be one id value for those columns in `cols` each
#     cols = ["legislature_id", "legislature_period"]
#     for c in cols:
#         ids = df[c].unique()
#         msg = f"Surprisingly found multiple {c} values: {ids}"
#         assert len(ids) == 1, msg

#     # there should be no duplicate mandate_id and politician_id values
#     cols = ["mandate_id", "politician_id"]
#     for c in cols:
#         mask = df[c].duplicated()
#         assert (
#             mask.sum() == 0
#         ), f"Surprisingly found duplicated {c} values: {df.loc[mask,c].unique()} \nexamples: \n{df.loc[mask].head()}"


def test_get_mandates_df():
    d = pd.DataFrame([MANDATE_DATA_PARSED])
    with patch(
        "bundestag.abgeordnetenwatch.load_mandate_json",
        MagicMock(return_value={"data": [MANDATE_DATA_RAW]}),
    ):
        res = aw.get_mandates_df(42, "dummy/path")
        assert res.equals(d)


@pytest.mark.parametrize(
    "dry,status_code",
    [
        (True, 200),
        (False, 200),
        (True, 201),
        (False, 201),
    ],
)
def test_get_vote_info(dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with patch(
        "requests.get", MagicMock(return_value=r)
    ) as _get, patch.object(r, "json", MagicMock()):
        # line to test
        try:
            aw.get_vote_info(42, dry=dry)
        except AssertionError as ex:
            if status_code != 200:
                pytest.xfail(
                    "Not 200 status_code value should raise an exception"
                )
            else:
                raise ex

        if dry:
            assert _get.call_count == 0
        else:
            assert _get.call_count == 1


def test_votes_file():
    assert aw.votes_file(42, 21) == "votes_legislature_42/poll_21_votes.json"


@pytest.mark.parametrize("dry", [True, False])
def test_store_vote_info(dry: bool):
    votes = {"data": {"field_legislature": {"id": 21}}}
    poll_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_vote_info(votes, poll_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _open.call_count == 1
            assert _mkdir.call_count == 1
            json_dump.assert_called_once()


@pytest.mark.parametrize("dry", [True, False])
def test_load_vote_json(dry: bool):
    legislature_id = 42
    poll_id = 21
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.load", MagicMock()) as json_load,
    ):
        # line to test
        aw.load_vote_json(legislature_id, poll_id, path)

        assert json_load.call_count == 1
        assert _open.call_count == 1
        assert _mkdir.call_count == 0


VOTE_DATA_RAW = {
    "id": 83519,
    "entity_type": "vote",
    "label": "Bernd Baumann - Verl\u00e4ngerung Ausbildungsfortsetzung Irak",
    "api_url": "https://www.abgeordnetenwatch.de/api/v2/votes/83519",
    "mandate": {
        "id": 45234,
        "entity_type": "candidacy_mandate",
        "label": "Bernd Baumann (Bundestag 2017 - 2021)",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/45234",
    },
    "poll": {
        "id": 1237,
        "entity_type": "node",
        "label": "Verl\u00e4ngerung Ausbildungsfortsetzung Irak",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/1237",
    },
    "vote": "no",
    "reason_no_show": None,
    "reason_no_show_other": None,
    "fraction": {
        "id": 56,
        "entity_type": "fraction",
        "label": "AfD (Bundestag 2017 - 2021)",
        "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/56",
    },
}

VOTE_DATA_PARSED = {
    "mandate_id": 45234,
    "mandate": "Bernd Baumann (Bundestag 2017 - 2021)",
    "poll_id": 1237,
    "vote": "no",
    "reason_no_show": None,
    "reason_no_show_other": None,
}


def test_parse_vote_data():
    res = aw.parse_vote_data(VOTE_DATA_RAW)

    assert all([k in res for k in VOTE_DATA_PARSED])
    for k in VOTE_DATA_PARSED.keys():
        assert VOTE_DATA_PARSED[k] == res[k]


# def test_vote_data(df):
#     "Basic sanity check on vote data"

#     # there should be no missing values for any column in `cols`
#     cols = ["mandate_id", "mandate", "poll_id", "vote"]
#     for c in cols:
#         msg = f"{c}: failed because NaNs/None values were found."
#         mask = df[c].isna()
#         assert mask.sum() == 0, f"{msg}: \n{df.loc[mask].head()}"

#     # there should only be one poll_id value
#     ids = df["poll_id"].unique()
#     msg = f"Surprisingly found multiple poll_id values: {ids}"
#     assert len(ids) == 1, msg

#     # there should be no duplicate mandate_id value
#     mask = df["mandate_id"].duplicated()
#     assert (
#         mask.sum() == 0
#     ), f'Surprisingly found duplicated mandate_id values: {df.loc[mask,"poll_id"].unique()} \nexamples: \n{df.loc[mask].head()}'


def test_get_votes_df():
    j = {"data": {"related_data": {"votes": [VOTE_DATA_RAW]}}}
    d = pd.DataFrame([VOTE_DATA_PARSED])
    with patch(
        "bundestag.abgeordnetenwatch.load_vote_json", MagicMock(return_value=j)
    ):
        res = aw.get_votes_df(42, 21, "dummy/path")
        assert res.equals(d)


@pytest.mark.skip("to be implemented")
def test_check_stored_vote_ids():
    pass


# def test_stored_vote_ids_check(path: Path = None):
#     tmp = check_stored_vote_ids(path=path)
#     assert isinstance(tmp, dict), "Sanity check for dict type of `tmp` failed"
#     assert all(
#         [isinstance(v, dict) for v in tmp.values()]
#     ), "Sanity check for dict type of values of `tmp` failed"
#     assert all(
#         [isinstance(p, Path) for d in tmp.values() for p in d.values()]
#     ), "Sanity check of lowest level values failed, expect all to be of type pathlib.Path"
@pytest.mark.skip("to be implemented")
def test_get_all_remaining_vote_info():
    pass


@pytest.mark.skip("to be implemented")
def test_compile_votes_data():
    pass


@pytest.mark.skip("to be implemented")
def test_get_party_from_fraction_string():
    pass


@pytest.mark.skip("to be implemented")
def test_get_politician_names():
    pass


@pytest.mark.skip("Currently not functioning because no data")
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
        self.num_mandates = (
            4  # number of mandates to collect if `self.dry = False`
        )
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
            polls = aw.load_polls_json(
                self.legislature_id, path=self.write_path
            )

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
            mandates = aw.load_mandate_json(
                self.legislature_id, self.write_path
            )

    def test_3_mandate_data(self):
        df = aw.get_mandates_df(self.legislature_id, path=self.write_path)
        aw.test_mandate_data(df)

    def test_4_get_vote_info(self):
        votes = aw.get_vote_info(self.poll_id, dry=self.dry)
        aw.store_vote_info(
            votes, self.poll_id, dry=self.dry, path=self.write_path
        )
        if not self.dry:
            votes = aw.load_vote_json(
                self.legislature_id, self.poll_id, path=self.write_path
            )

    def test_5_vote_data(self):
        df = aw.get_votes_df(
            self.legislature_id, self.poll_id, path=self.write_path
        )
        aw.test_vote_data(df)

    def test_6_stored_vote_ids(self):
        aw.test_stored_vote_ids_check(path=self.write_path)


if __name__ == "__main__":
    unittest.main()
