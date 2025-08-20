import json
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pandas as pd
import pandera as pa
import pytest

# import bundestag.data.transform.abgeordnetenwatch as aw
import bundestag.schemas as schemas
from bundestag.data.transform.abgeordnetenwatch.process import (
    compile_votes_data,
    get_mandates_data,
    get_polls_data,
    get_votes_data,
    load_mandate_json,
    load_polls_json,
    load_vote_json,
    parse_mandate_data,
    parse_poll_data,
    parse_vote_data,
)
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
        load_polls_json(legislature_id, path, dry=dry)

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
        load_mandate_json(legislature_id, path, dry=dry)

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
        load_vote_json(legislature_id, poll_id, path)

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
    res = parse_poll_data(data)

    assert set(list(POLL_DATA_PARSED.keys())) == set(list(res.keys()))
    for k in POLL_DATA_PARSED.keys():
        assert POLL_DATA_PARSED[k] == res[k]


@pytest.mark.skip("Validation currently fails.")
@pytest.mark.parametrize("fraction_membership_is_none", [True, False])
def test_parse_mandate_response(
    fraction_membership_is_none: bool, mandates_response_raw: dict
):
    response = schemas.MandatesResponse(**mandates_response_raw)

    data = response.data[0]
    if fraction_membership_is_none:
        data.fraction_membership = None

    res = parse_mandate_data(data, missing="missing")

    assert all([k in res for k in MANDATE_DATA_PARSED])
    for k in MANDATE_DATA_PARSED.keys():
        if fraction_membership_is_none and k.startswith("fraction"):
            assert res[k] == ["missing"]
        else:
            assert res[k] == MANDATE_DATA_PARSED[k]


@pytest.mark.skip("Validation currently failing")
def test_parse_vote_response(votes_response_raw):
    response = schemas.VoteResponse(**votes_response_raw)
    data = response.data.related_data.votes[0]
    res = parse_vote_data(data)

    assert all([k in res for k in VOTE_DATA_PARSED])
    for k in VOTE_DATA_PARSED.keys():
        assert VOTE_DATA_PARSED[k] == res[k]


@pytest.mark.skip("mocking broken")
def test_get_polls_df(poll_response_raw: dict):
    with patch(
        "bundestag.data.transform.abgeordnetenwatch.load_polls_json",
        MagicMock(return_value=poll_response_raw),
    ):
        res = get_polls_data(42, "dummy/path")
        assert res.equals(POLLS_DF)


@pytest.mark.skip("Validation currently failing")
def test_get_mandates_df(mandates_response_raw: dict):
    with patch(
        "bundestag.data.transform.abgeordnetenwatch.load_mandate_json",
        MagicMock(return_value=mandates_response_raw),
    ):
        res = get_mandates_data(42, "dummy/path")
        assert res.equals(MANDATES_DF)


@pytest.mark.skip("Validation currently failing")
@pytest.mark.parametrize(
    "has_none,validate",
    [(has_none, validate) for has_none in [False, True] for validate in [False, True]],
)
def test_get_votes_data(has_none: int, validate: bool, votes_response_raw: dict):
    data = votes_response_raw.copy()
    if has_none:
        data["data"]["related_data"]["votes"][0]["id"] = None

    with patch(
        "bundestag.data.transform.abgeordnetenwatch.load_vote_json",
        MagicMock(return_value=data),
    ) as _load_vote_json:
        # line to test
        res = get_votes_data(42, 21, "dummy/path", validate=validate)

        _load_vote_json.assert_called_once_with(42, 21, path="dummy/path")

        if has_none:
            assert len(res) == 1
            assert res.iloc[0].equals(VOTES_DF.iloc[1])
        else:
            assert len(res) == 2
            assert res.equals(VOTES_DF)


@pytest.mark.skip("mocking and pandera broken")
@pytest.mark.parametrize(
    "n,has_duplicate",
    [(n, has_duplicate) for n in [1, 2] for has_duplicate in [False, True]],
)
def test_compile_votes_data(n: int, has_duplicate: bool):
    votes_df = VOTES_DF.copy()
    if has_duplicate:
        votes_df = pd.concat([votes_df, votes_df], ignore_index=True)

    with (
        patch(
            "bundestag.data.download.abgeordnetenwatch.check_stored_vote_ids",
            MagicMock(return_value={42: list(range(n))}),
        ) as _check_stored_vote_ids,
        patch(
            "bundestag.data.transform.abgeordnetenwatch.get_votes_df",
            MagicMock(return_value=votes_df),
        ) as _get_votes_df,
    ):
        try:
            # line to test
            res = compile_votes_data(42, "dummy/path", validate=True)
        except pa.errors.SchemaError as ex:
            if n > 1:
                pytest.xfail(
                    "Duplicate poll_id-mandate_id combinations lead to schema error"
                )
            else:
                raise ex

        _check_stored_vote_ids.assert_called_once_with(
            legislature_id=42, path="dummy/path"
        )
        assert _get_votes_df.call_count == n
        if has_duplicate:
            assert len(res) == len(votes_df) / 2
        else:
            assert len(res) == len(votes_df)


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
