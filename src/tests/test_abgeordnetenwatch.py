import json
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest
import requests

import bundestag.abgeordnetenwatch as aw
import bundestag.schemas as schemas


def test_get_location():
    fname = "wup.csv"
    path = Path("file/location")

    file = aw.get_location(fname=fname, path=path, dry=True, mkdir=False)
    assert file == path / fname


@pytest.fixture(scope="module")
def poll_response_raw() -> dict:
    response = json.load(
        open("src/tests/data/polls_legislature_111.json", "r")
    )
    return response


@pytest.fixture(scope="module")
def mandates_response_raw() -> dict:
    response = json.load(
        open("src/tests/data/mandates_legislature_111.json", "r")
    )
    return response


@pytest.fixture(scope="module")
def votes_response_raw() -> dict:
    response = json.load(open("src/tests/data/poll_4217_votes.json", "r"))
    return response


POLL_DATA_PARSED = {
    "poll_id": 4293,
    "poll_title": "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
    "poll_first_committee": "Haushaltsausschuss",
    "poll_description": "Ein von den Fraktionen der CDU/CSU und SPD eingebrachter Gesetzentwurf zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.\nIm Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.\nDie Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD angenommen. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.",
    "legislature_id": 111,
    "legislature_period": "Bundestag 2017 - 2021",
    "poll_date": "2021-09-07",
}

MANDATE_DATA_PARSED = {
    "legislature_id": 111,
    "legislature_period": "Bundestag 2017 - 2021",
    "mandate_id": 52657,
    "mandate": "Zeki Gökhan (Bundestag 2017 - 2021)",
    "politician_id": 122163,
    "politician": "Zeki Gökhan",
    "politician_url": "https://www.abgeordnetenwatch.de/profile/zeki-goekhan",
    "start_date": "2021-08-19",
    "end_date": None,
    "constituency_id": 4215,
    "constituency_name": "91 - Rhein-Erft-Kreis I (Bundestag 2017 - 2021)",
    "fraction_names": ["DIE LINKE seit 19.08.2021"],
    "fraction_ids": [9233],
    "fraction_starts": ["2021-08-19"],
    "fraction_ends": [None],
}

VOTE_DATA_PARSED = {
    "mandate_id": 45467,
    "mandate": "Michael von Abercron (Bundestag 2017 - 2021)",
    "poll_id": 4217,
    "vote": "yes",
    "reason_no_show": None,
    "reason_no_show_other": None,
}

POLLS_DF = pd.DataFrame(
    {
        "poll_id": {0: 4293, 1: 4284},
        "poll_title": {
            0: "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
            1: "Fortbestand der epidemischen Lage von nationaler Tragweite",
        },
        "poll_first_committee": {
            0: "Haushaltsausschuss",
            1: "Ausschuss für Gesundheit",
        },
        "poll_description": {
            0: "Ein von den Fraktionen der CDU/CSU und SPD eingebrachter Gesetzentwurf zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.\nIm Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.\nDie Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD angenommen. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.",
            1: "Der von den Fraktionen der CDU/CSU und SPD eingebrachte Antrag sieht vor, dass der Bundestag feststellt, dass die seit dem 25. März 2020 geltende epidemische Lage von nationaler Tragweite weiter fortbesteht. Das wird damit begründet, dass angesichts des erneuten Anstiegs der COVID-19-Fallzahlen in Deutschland weiterhin eine erhebliche Gesundheitsgefährdung der Bevölkerung gegeben sei.\nDer Antrag wurde mit 325 Ja-Stimmen der CDU- und SPD-Fraktion gegen 252 Nein-Stimmen der Oppositionsfraktionen angenommen. Fünf Abgeordnete haben sich enthalten.",
        },
        "legislature_id": {0: 111, 1: 111},
        "legislature_period": {
            0: "Bundestag 2017 - 2021",
            1: "Bundestag 2017 - 2021",
        },
        "poll_date": {0: "2021-09-07", 1: "2021-08-25"},
    }
)

MANDATES_DF = pd.DataFrame(
    {
        "legislature_id": {0: 111, 1: 111},
        "legislature_period": {
            0: "Bundestag 2017 - 2021",
            1: "Bundestag 2017 - 2021",
        },
        "mandate_id": {0: 52657, 1: 52107},
        "mandate": {
            0: "Zeki Gökhan (Bundestag 2017 - 2021)",
            1: "Florian Jäger (Bundestag 2017 - 2021)",
        },
        "politician_id": {0: 122163, 1: 121214},
        "politician": {0: "Zeki Gökhan", 1: "Florian Jäger"},
        "politician_url": {
            0: "https://www.abgeordnetenwatch.de/profile/zeki-goekhan",
            1: "https://www.abgeordnetenwatch.de/profile/florian-jaeger",
        },
        "start_date": {0: "2021-08-19", 1: "2021-07-20"},
        "end_date": {0: None, 1: None},
        "constituency_id": {0: 4215, 1: 4339},
        "constituency_name": {
            0: "91 - Rhein-Erft-Kreis I (Bundestag 2017 - 2021)",
            1: "215 - Fürstenfeldbruck (Bundestag 2017 - 2021)",
        },
        "fraction_names": {
            0: ["DIE LINKE seit 19.08.2021"],
            1: ["AfD seit 20.07.2021"],
        },
        "fraction_ids": {0: [9233], 1: [9228]},
        "fraction_starts": {0: ["2021-08-19"], 1: ["2021-07-20"]},
        "fraction_ends": {0: [None], 1: [None]},
    }
)

VOTES_DF = pd.DataFrame(
    {
        "mandate_id": {0: 45467, 1: 44472},
        "mandate": {
            0: "Michael von Abercron (Bundestag 2017 - 2021)",
            1: "Stephan Albani (Bundestag 2017 - 2021)",
        },
        "poll_id": {0: 4217, 1: 4217},
        "vote": {0: "yes", 1: "yes"},
        "reason_no_show": {0: None, 1: None},
        "reason_no_show_other": {0: None, 1: None},
    }
)


def test_parse_poll_response(poll_response_raw: dict):
    response = schemas.PollResponse(**poll_response_raw)
    data = response.data[0]
    res = aw.parse_poll_data(data)

    assert set(list(POLL_DATA_PARSED.keys())) == set(list(res.keys()))
    for k in POLL_DATA_PARSED.keys():
        assert POLL_DATA_PARSED[k] == res[k]


def test_parse_mandate_response(mandates_response_raw: dict):
    response = schemas.MandatesResponse(**mandates_response_raw)
    data = response.data[0]
    res = aw.parse_mandate_data(data)

    assert all([k in res for k in MANDATE_DATA_PARSED])
    for k in MANDATE_DATA_PARSED.keys():
        assert MANDATE_DATA_PARSED[k] == res[k]


def test_parse_vote_response(votes_response_raw):
    response = schemas.VoteResponse(**votes_response_raw)
    data = response.data.related_data.votes[0]
    res = aw.parse_vote_data(data)

    assert all([k in res for k in VOTE_DATA_PARSED])
    for k in VOTE_DATA_PARSED.keys():
        assert VOTE_DATA_PARSED[k] == res[k]


def test_get_polls_df(poll_response_raw: dict):
    with patch(
        "bundestag.abgeordnetenwatch.load_polls_json",
        MagicMock(return_value=poll_response_raw),
    ):
        res = aw.get_polls_df(42, "dummy/path")
        assert res.equals(POLLS_DF)


def test_get_mandates_df(mandates_response_raw: dict):
    with patch(
        "bundestag.abgeordnetenwatch.load_mandate_json",
        MagicMock(return_value=mandates_response_raw),
    ):
        res = aw.get_mandates_df(42, "dummy/path")
        assert res.equals(MANDATES_DF)


def test_get_votes_df(votes_response_raw: dict):
    with patch(
        "bundestag.abgeordnetenwatch.load_vote_json",
        MagicMock(return_value=votes_response_raw),
    ):
        res = aw.get_votes_df(42, 21, "dummy/path")
        assert res.equals(VOTES_DF)


@pytest.mark.parametrize(
    "func,dry,status_code",
    [
        (func, dry, status_code)
        for func in [
            aw.request_poll_data,
            aw.request_mandates_data,
            aw.request_vote_data,
        ]
        for dry in [True, False]
        for status_code in [200, 201]
    ],
)
def test_request_data(func: T.Callable, dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with patch(
        "requests.get", MagicMock(return_value=r)
    ) as _get, patch.object(r, "json", MagicMock()):
        # line to test
        try:
            func(42, dry=dry)
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


def test_mandates_file():
    assert aw.mandates_file(42) == "mandates_legislature_42.json"


def test_votes_file():
    assert aw.votes_file(42, 21) == "votes_legislature_42/poll_21_votes.json"


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
def test_store_mandates_json(dry: bool):
    polls = {}
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_mandates_json(polls, legislature_id, dry=dry, path=path)

        assert _mkdir.call_count == 0
        if dry:
            assert _open.call_count == 0
        else:
            assert _open.call_count == 1
            json_dump.assert_called_once()


@pytest.mark.parametrize("dry", [True, False])
def test_store_vote_json(dry: bool):
    votes = {"data": {"field_legislature": {"id": 21}}}
    poll_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_vote_json(votes, poll_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _open.call_count == 1
            assert _mkdir.call_count == 1
            json_dump.assert_called_once()


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


# VOTE_DATA_RAW = {
#     "id": 83519,
#     "entity_type": "vote",
#     "label": "Bernd Baumann - Verl\u00e4ngerung Ausbildungsfortsetzung Irak",
#     "api_url": "https://www.abgeordnetenwatch.de/api/v2/votes/83519",
#     "mandate": {
#         "id": 45234,
#         "entity_type": "candidacy_mandate",
#         "label": "Bernd Baumann (Bundestag 2017 - 2021)",
#         "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/45234",
#     },
#     "poll": {
#         "id": 1237,
#         "entity_type": "node",
#         "label": "Verl\u00e4ngerung Ausbildungsfortsetzung Irak",
#         "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/1237",
#     },
#     "vote": "no",
#     "reason_no_show": None,
#     "reason_no_show_other": None,
#     "fraction": {
#         "id": 56,
#         "entity_type": "fraction",
#         "label": "AfD (Bundestag 2017 - 2021)",
#         "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/56",
#     },
# }

# VOTE_DATA_PARSED = {
#     "mandate_id": 45234,
#     "mandate": "Bernd Baumann (Bundestag 2017 - 2021)",
#     "poll_id": 1237,
#     "vote": "no",
#     "reason_no_show": None,
#     "reason_no_show_other": None,
# }


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
        polls = aw.request_poll_data(
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
        mandates = aw.request_mandates_data(
            self.legislature_id, dry=self.dry, num_mandates=self.num_mandates
        )
        aw.store_mandates_json(
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
        votes = aw.request_vote_data(self.poll_id, dry=self.dry)
        aw.store_vote_json(
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
