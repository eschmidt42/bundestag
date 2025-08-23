import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from bundestag.data.transform.abgeordnetenwatch.transform import (
    run,
    transform_mandates_data,
    transform_votes_data,
)


@pytest.fixture(scope="module")
def poll_response_raw() -> dict:
    response = json.load(open("tests/data_for_testing/polls_legislature_111.json", "r"))
    return response


@pytest.fixture(scope="module")
def mandates_response_raw() -> dict:
    response = json.load(
        open("tests/data_for_testing/mandates_legislature_111.json", "r")
    )
    return response


@pytest.fixture(scope="module")
def votes_response_raw() -> dict:
    response = json.load(open("tests/data_for_testing/poll_4217_votes.json", "r"))
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


def test_transform_mandates_data():
    df = MANDATES_DF.copy()
    # line to test
    res = transform_mandates_data(df)
    assert "all_parties" in res.columns
    assert "party" in res.columns


def test_transform_votes_data():
    df = VOTES_DF.copy()
    # line to test
    res = transform_votes_data(df)
    assert "politician name" in res.columns
    assert res.drop(columns=["politician name"]).equals(VOTES_DF)


@pytest.mark.skip("mocking broken")
@pytest.mark.parametrize(
    "dry,raw_path_exists,preprocessed_path_exists,raw_path,preprocessed_path,validate",
    [
        (
            dry,
            raw_path_exists,
            preprocessed_path_exists,
            raw_path,
            preprocessed_path,
            validate,
        )
        for dry in [True, False]
        for raw_path_exists in [True, False]
        for preprocessed_path_exists in [True, False]
        for raw_path in [Path("raw/path"), None]
        for preprocessed_path in [Path("preprocessed/path"), None]
        for validate in [True, False]
    ],
)
def test_run(
    dry: bool,
    raw_path_exists: bool,
    preprocessed_path_exists: bool,
    raw_path: Path,
    preprocessed_path: Path,
    validate: bool,
):
    legislature_id = 42

    with (
        patch(
            "pathlib.Path.exists",
            MagicMock(side_effect=[raw_path_exists, preprocessed_path_exists]),
        ) as _exists,
        patch("bundestag.data.utils.ensure_path_exists", MagicMock()) as _ensure_exists,
        patch("pandas.DataFrame.to_parquet", MagicMock()) as _to_parquet,
        patch("pandas.DataFrame.to_csv", MagicMock()) as _to_csv,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.get_polls_data",
            MagicMock(return_value=POLLS_DF),
        ) as _get_polls_data,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.get_mandates_data",
            MagicMock(return_value=MANDATES_DF),
        ) as _get_mandates_data,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.transform_mandates_data",
            MagicMock(return_value=MANDATES_DF),
        ) as _transform_mandates_data,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.compile_votes_data",
            MagicMock(return_value={}),
        ) as _compile_votes_data,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.transform_votes_data",
            MagicMock(return_value=pd.DataFrame()),
        ) as _transform_votes_data,
    ):
        try:
            # line to test
            run(
                legislature_id=legislature_id,
                dry=dry,
                raw_path=raw_path,
                preprocessed_path=preprocessed_path,
                validate=validate,
            )
        except ValueError as ex:
            if not dry and (raw_path is None or preprocessed_path is None):
                pytest.xfail("ValueError for missing path")
            elif not dry and (not raw_path_exists or not preprocessed_path_exists):
                pytest.xfail("ValueError for existing raw and preprocessed data path")
            elif not raw_path_exists:
                pytest.xfail("ValueError for missing raw data path")
            else:
                raise ex

        if not dry:
            assert _exists.call_count == 2
        if not dry and not preprocessed_path_exists:
            _ensure_exists.assert_called_once_with(preprocessed_path)

        if not dry:
            _to_parquet.assert_has_calls(
                [
                    call(path=preprocessed_path / f"df_polls_{legislature_id}.parquet"),
                    call(
                        path=preprocessed_path / f"df_mandates_{legislature_id}.parquet"
                    ),
                    call(
                        path=preprocessed_path
                        / f"df_all_votes_{legislature_id}.parquet"
                    ),
                ]
            )
            _to_csv.assert_has_calls(
                [
                    call(
                        preprocessed_path
                        / f"compiled_votes_legislature_{legislature_id}.csv",
                        index=False,
                    )
                ]
            )

        _get_polls_data.assert_called_once_with(legislature_id, path=raw_path)

        _get_mandates_data.assert_called_once_with(legislature_id, path=raw_path)
        _transform_mandates_data.assert_called_once_with(MANDATES_DF)

        _compile_votes_data.assert_called_once_with(
            legislature_id, path=raw_path, validate=validate
        )
        _transform_votes_data.assert_called_once_with({})
