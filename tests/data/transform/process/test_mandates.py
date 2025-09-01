from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

import bundestag.schemas as schemas
from bundestag.data.transform.abgeordnetenwatch.process.mandates import (
    get_mandates_data,
    load_mandate_json,
    parse_mandate_data,
)


def test_get_mandates_data(sample_mandates_json_path: Path, MANDATES_DF: pl.DataFrame):
    legislature_id = 111
    res = get_mandates_data(legislature_id, sample_mandates_json_path.parent)
    assert res.equals(MANDATES_DF)


@pytest.mark.parametrize("no_fraction_membership", [True, False])
def test_parse_mandate_data(
    no_fraction_membership: bool,
    sample_mandates_response: schemas.MandatesResponse,
    MANDATE_DATA_PARSED: dict,
):
    data = sample_mandates_response.data[0]
    if no_fraction_membership:
        data.fraction_membership = []

    res = parse_mandate_data(data, missing="missing")

    assert all([k in res for k in MANDATE_DATA_PARSED])
    for k in MANDATE_DATA_PARSED.keys():
        if no_fraction_membership and k.startswith("fraction"):
            assert res[k] == snapshot(["missing"])
        else:
            assert res[k] == MANDATE_DATA_PARSED[k]


def test_load_mandate_json(sample_mandates_json_path: Path):
    legislature_id = 111
    res = load_mandate_json(legislature_id, sample_mandates_json_path.parent)
    assert res == snapshot(
        {
            "meta": {
                "abgeordnetenwatch_api": {
                    "version": "2.4",
                    "changelog": "https://www.abgeordnetenwatch.de/api/version-changelog/aktuell",
                    "licence": "CC0 1.0",
                    "licence_link": "https://creativecommons.org/publicdomain/zero/1.0/deed.de",
                    "documentation": "https://www.abgeordnetenwatch.de/api/entitaeten/candidacy-mandate",
                },
                "status": "ok",
                "status_message": "",
                "result": {
                    "count": 708,
                    "total": 708,
                    "range_start": 0,
                    "range_end": 999,
                },
            },
            "data": [
                {
                    "id": 52657,
                    "entity_type": "candidacy_mandate",
                    "label": "Zeki Gökhan (Bundestag 2017 - 2021)",
                    "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/52657",
                    "id_external_administration": "2312",
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
                        "id": 122163,
                        "entity_type": "politician",
                        "label": "Zeki Gökhan",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/politicians/122163",
                        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/profile/zeki-goekhan",
                    },
                    "start_date": "2021-08-19",
                    "end_date": None,
                    "info": None,
                    "electoral_data": {
                        "id": 53581,
                        "entity_type": "electoral_data",
                        "label": "Zeki Gökhan (Bundestag 2017 - 2021)",
                        "electoral_list": {
                            "id": 115,
                            "entity_type": "electoral_list",
                            "label": "Landesliste Nordrhein-Westfalen (Bundestag 2017 - 2021)",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/electoral-lists/115",
                        },
                        "list_position": 14,
                        "constituency": {
                            "id": 4215,
                            "entity_type": "constituency",
                            "label": "91 - Rhein-Erft-Kreis I (Bundestag 2017 - 2021)",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/constituencies/4215",
                        },
                        "constituency_result": 4.5,
                        "constituency_result_count": None,
                        "mandate_won": "moved_up",
                    },
                    "fraction_membership": [
                        {
                            "id": 9233,
                            "entity_type": "fraction_membership",
                            "label": "DIE LINKE seit 19.08.2021",
                            "fraction": {
                                "id": 41,
                                "entity_type": "fraction",
                                "label": "DIE LINKE (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/41",
                            },
                            "valid_from": "2021-08-19",
                            "valid_until": None,
                        }
                    ],
                },
                {
                    "id": 52107,
                    "entity_type": "candidacy_mandate",
                    "label": "Florian Jäger (Bundestag 2017 - 2021)",
                    "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/52107",
                    "id_external_administration": "2311",
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
                        "id": 121214,
                        "entity_type": "politician",
                        "label": "Florian Jäger",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/politicians/121214",
                        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/profile/florian-jaeger",
                    },
                    "start_date": "2021-07-20",
                    "end_date": None,
                    "info": None,
                    "electoral_data": {
                        "id": 53029,
                        "entity_type": "electoral_data",
                        "label": "Florian Jäger (Bundestag 2017 - 2021)",
                        "electoral_list": None,
                        "list_position": None,
                        "constituency": {
                            "id": 4339,
                            "entity_type": "constituency",
                            "label": "215 - Fürstenfeldbruck (Bundestag 2017 - 2021)",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/constituencies/4339",
                        },
                        "constituency_result": None,
                        "constituency_result_count": None,
                        "mandate_won": "moved_up",
                    },
                    "fraction_membership": [
                        {
                            "id": 9228,
                            "entity_type": "fraction_membership",
                            "label": "AfD seit 20.07.2021",
                            "fraction": {
                                "id": 56,
                                "entity_type": "fraction",
                                "label": "AfD (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/56",
                            },
                            "valid_from": "2021-07-20",
                            "valid_until": None,
                        }
                    ],
                },
            ],
        }
    )
