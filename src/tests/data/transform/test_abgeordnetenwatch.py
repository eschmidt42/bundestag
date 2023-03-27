import json
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pandera as pa
import pytest
import requests

import bundestag.data.transform.abgeordnetenwatch as aw
import bundestag.schemas as schemas


@pytest.fixture(scope="module")
def poll_response_raw() -> dict:
    response = json.load(
        open("src/tests/data_for_testing/polls_legislature_111.json", "r")
    )
    return response


@pytest.fixture(scope="module")
def mandates_response_raw() -> dict:
    response = json.load(
        open("src/tests/data_for_testing/mandates_legislature_111.json", "r")
    )
    return response


@pytest.fixture(scope="module")
def votes_response_raw() -> dict:
    response = json.load(
        open("src/tests/data_for_testing/poll_4217_votes.json", "r")
    )
    return response


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
    "end_date": "",
    "constituency_id": 4215,
    "constituency_name": "91 - Rhein-Erft-Kreis I (Bundestag 2017 - 2021)",
    "fraction_names": ["DIE LINKE seit 19.08.2021"],
    "fraction_ids": [9233],
    "fraction_starts": ["2021-08-19"],
    "fraction_ends": [""],
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
        "end_date": {0: "", 1: ""},
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
        "fraction_ends": {0: [""], 1: [""]},
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
        "bundestag.data.transform.abgeordnetenwatch.load_polls_json",
        MagicMock(return_value=poll_response_raw),
    ):
        res = aw.get_polls_df(42, "dummy/path")
        assert res.equals(POLLS_DF)


def test_get_mandates_df(mandates_response_raw: dict):
    with patch(
        "bundestag.data.transform.abgeordnetenwatch.load_mandate_json",
        MagicMock(return_value=mandates_response_raw),
    ):
        res = aw.get_mandates_df(42, "dummy/path")
        assert res.equals(MANDATES_DF)


def test_get_votes_df(votes_response_raw: dict):
    with patch(
        "bundestag.data.transform.abgeordnetenwatch.load_vote_json",
        MagicMock(return_value=votes_response_raw),
    ):
        res = aw.get_votes_df(42, 21, "dummy/path")
        assert res.equals(VOTES_DF)


@pytest.mark.parametrize("n", [1, 2])
def test_compile_votes_data(n):
    with patch(
        "bundestag.data.transform.abgeordnetenwatch.get_votes_df",
        MagicMock(return_value=VOTES_DF),
    ), patch(
        "bundestag.data.download.abgeordnetenwatch.check_stored_vote_ids",
        MagicMock(return_value={42: list(range(n))}),
    ):
        # line to test
        try:
            _ = aw.compile_votes_data(42, "dummy/path", validate=True)
        except pa.errors.SchemaError as ex:
            if n > 1:
                pytest.xfail(
                    "Duplicate poll_id-mandate_id combinations lead to schema error"
                )
            else:
                raise ex


@pytest.mark.parametrize(
    "entries,targets",
    [
        (["DIE LINKE seit 19.08.2021"], ["DIE LINKE"]),
        (
            ["AfD seit 20.07.2021", "CDU/CSU seit 01.07.2021"],
            ["AfD", "CDU/CSU"],
        ),
        ([], []),
        (None, ["unknown"]),
    ],
)
def test_get_parties_from_col(entries: T.List[str], targets: T.List[str]):
    col = "fraction_names"
    row = pd.Series({col: entries})
    # line to test
    res = aw.get_parties_from_col(row, col=col, missing="unknown")
    assert all([targ == res[i] for i, targ in enumerate(targets)])


def test_get_politician_names():
    col = "mandate"
    df = pd.DataFrame(
        {
            col: [
                "Zeki Gökhan (Bundestag 2017 - 2021)",
                "bla blaaaa blah (Bundestag 2017 - 2021)",
                "wup (Bundestag 2022 - 2025)",
            ]
        }
    )

    # line to test
    names = aw.get_politician_names(df, col=col)

    assert names[0] == "Zeki Gökhan"
    assert names[1] == "bla blaaaa blah"
    assert names[2] == "wup"
