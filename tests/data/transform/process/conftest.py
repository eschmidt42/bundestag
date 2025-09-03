import json
from pathlib import Path

import polars as pl
import pytest

import bundestag.schemas as schemas


@pytest.fixture()
def sample_poll_json_path() -> Path:
    return Path("tests/data_for_testing/polls_legislature_111.json")


@pytest.fixture()
def sample_mandates_json_path() -> Path:
    return Path("tests/data_for_testing/mandates_legislature_111.json")


@pytest.fixture()
def sample_vote_json_path() -> Path:
    return Path("tests/data_for_testing/votes_legislature_111/poll_4217_votes.json")


@pytest.fixture()
def sample_poll_response(sample_poll_json_path: Path) -> schemas.PollResponse:
    with sample_poll_json_path.open("r") as f:
        response = json.load(f)
    return schemas.PollResponse(**response)


@pytest.fixture()
def sample_poll(sample_poll_response: schemas.PollResponse) -> schemas.Poll:
    return sample_poll_response.data[0]


@pytest.fixture()
def sample_mandates_response(
    sample_mandates_json_path: Path,
) -> schemas.MandatesResponse:
    with sample_mandates_json_path.open("r") as f:
        response = json.load(f)
    return schemas.MandatesResponse(**response)


@pytest.fixture()
def sample_votes_response(sample_vote_json_path: Path) -> schemas.VoteResponse:
    with sample_vote_json_path.open("r") as f:
        response = json.load(f)
    return schemas.VoteResponse(**response)


@pytest.fixture()
def POLLS_DF() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "poll_id": [4293, 4284],
            "poll_title": [
                "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
                "Fortbestand der epidemischen Lage von nationaler Tragweite",
            ],
            "poll_first_committee": [
                "Haushaltsausschuss",
                "Ausschuss für Gesundheit",
            ],
            "poll_description": [
                "Ein von den Fraktionen der CDU/CSU und SPD eingebrachter Gesetzentwurf zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.\nIm Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.\nDie Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD angenommen. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.",
                "Der von den Fraktionen der CDU/CSU und SPD eingebrachte Antrag sieht vor, dass der Bundestag feststellt, dass die seit dem 25. März 2020 geltende epidemische Lage von nationaler Tragweite weiter fortbesteht. Das wird damit begründet, dass angesichts des erneuten Anstiegs der COVID-19-Fallzahlen in Deutschland weiterhin eine erhebliche Gesundheitsgefährdung der Bevölkerung gegeben sei.\nDer Antrag wurde mit 325 Ja-Stimmen der CDU- und SPD-Fraktion gegen 252 Nein-Stimmen der Oppositionsfraktionen angenommen. Fünf Abgeordnete haben sich enthalten.",
            ],
            "legislature_id": [111, 111],
            "legislature_period": [
                "Bundestag 2017 - 2021",
                "Bundestag 2017 - 2021",
            ],
            "poll_date": ["2021-09-07", "2021-08-25"],
        }
    )


@pytest.fixture()
def MANDATES_DF() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "legislature_id": [111, 111],
            "legislature_period": [
                "Bundestag 2017 - 2021",
                "Bundestag 2017 - 2021",
            ],
            "mandate_id": [52657, 52107],
            "mandate": [
                "Zeki Gökhan (Bundestag 2017 - 2021)",
                "Florian Jäger (Bundestag 2017 - 2021)",
            ],
            "politician_id": [122163, 121214],
            "politician": ["Zeki Gökhan", "Florian Jäger"],
            "politician_url": [
                "https://www.abgeordnetenwatch.de/profile/zeki-goekhan",
                "https://www.abgeordnetenwatch.de/profile/florian-jaeger",
            ],
            "start_date": ["2021-08-19", "2021-07-20"],
            "end_date": ["", ""],
            "constituency_id": [4215, 4339],
            "constituency_name": [
                "91 - Rhein-Erft-Kreis I (Bundestag 2017 - 2021)",
                "215 - Fürstenfeldbruck (Bundestag 2017 - 2021)",
            ],
            "fraction_names": [
                ["DIE LINKE seit 19.08.2021"],
                ["AfD seit 20.07.2021"],
            ],
            "fraction_ids": [[9233], [9228]],
            "fraction_starts": [["2021-08-19"], ["2021-07-20"]],
            "fraction_ends": [[""], [""]],
        }
    )


@pytest.fixture()
def VOTES_DF() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mandate_id": [45467, 44472],
            "mandate": [
                "Michael von Abercron (Bundestag 2017 - 2021)",
                "Stephan Albani (Bundestag 2017 - 2021)",
            ],
            "poll_id": [4217, 4217],
            "vote": ["yes", "yes"],
            "reason_no_show": [None, None],
            "reason_no_show_other": [None, None],
        }
    )


@pytest.fixture()
def POLL_DATA_PARSED() -> dict:
    return {
        "poll_id": 4293,
        "poll_title": "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
        "poll_first_committee": "Haushaltsausschuss",
        "poll_description": "Ein von den Fraktionen der CDU/CSU und SPD eingebrachter Gesetzentwurf zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.\nIm Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.\nDie Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD angenommen. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.",
        "legislature_id": 111,
        "legislature_period": "Bundestag 2017 - 2021",
        "poll_date": "2021-09-07",
    }


@pytest.fixture()
def MANDATE_DATA_PARSED() -> dict:
    return {
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


@pytest.fixture()
def VOTE_DATA_PARSED() -> dict:
    return {
        "mandate_id": 45467,
        "mandate": "Michael von Abercron (Bundestag 2017 - 2021)",
        "poll_id": 4217,
        "vote": "yes",
        "reason_no_show": None,
        "reason_no_show_other": None,
    }
