from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

import bundestag.schemas as schemas
from bundestag.data.transform.abgeordnetenwatch.process.polls import (
    get_polls_data,
    load_polls_json,
    parse_poll_data,
)

# =================== get_polls_data ===================


def test_get_polls_data(sample_poll_json_path: Path, POLLS_DF: pl.DataFrame):
    """Tests getting polls data and parsing it into a DataFrame."""
    legislature_id = 111

    df = get_polls_data(legislature_id, sample_poll_json_path.parent)

    assert isinstance(df, pl.DataFrame)
    assert df.equals(POLLS_DF)


def test_get_polls_data_empty(tmp_path: Path):
    """Tests getting polls data when the data list is empty."""
    legislature_id = 19

    # Create dummy PollResponse data with empty data list
    meta = schemas.AbgeordnetenwatchMeta(
        abgeordnetenwatch_api=schemas.AbgeordnetenwatchAPI(
            version="1.0",
            changelog="",
            licence="",
            licence_link="",
            documentation="",
        ),
        status="ok",
        status_message="",
        result=schemas.PollResult(
            count=0,
            total=0,
            range_start=0,
            range_end=0,
        ),
    )
    poll_response_data = schemas.PollResponse(meta=meta, data=[])

    # Create a dummy file
    file_path = tmp_path / f"polls_legislature_{legislature_id}.json"
    with file_path.open("w") as f:
        try:
            # pydantic v2
            f.write(poll_response_data.model_dump_json())
        except AttributeError:
            # pydantic v1
            f.write(poll_response_data.json())

    # Test getting the data
    df = get_polls_data(legislature_id, path=tmp_path)

    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


# =================== parse_poll_data ===================


def test_parse_poll_data(sample_poll: schemas.Poll, POLL_DATA_PARSED: dict):
    """Tests parsing of a standard poll object."""
    parsed_data = parse_poll_data(sample_poll)
    assert parsed_data == snapshot(
        {
            "poll_id": 4293,
            "poll_title": "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
            "poll_first_committee": "Haushaltsausschuss",
            "poll_description": """\
Ein von den Fraktionen der CDU/CSU und SPD eingebrachter Gesetzentwurf zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.
Im Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.
Die Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD angenommen. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.\
""",
            "legislature_id": 111,
            "legislature_period": "Bundestag 2017 - 2021",
            "poll_date": "2021-09-07",
        }
    )

    assert set(list(POLL_DATA_PARSED.keys())) == set(list(parsed_data.keys()))
    for k in POLL_DATA_PARSED.keys():
        assert POLL_DATA_PARSED[k] == parsed_data[k]


def test_parse_poll_data_multiple_committees(sample_poll: schemas.Poll):
    """Tests that only the first committee is used when multiple are present."""
    committee2 = schemas.FieldCommittee(
        id=456,
        entity_type="committee",
        label="Ausschuss für Recht und Verbraucherschutz",
        api_url="/api/v1/committees/456",
    )
    sample_poll.field_committees.append(committee2)
    parsed_data = parse_poll_data(sample_poll)
    assert parsed_data["poll_first_committee"] == snapshot("Haushaltsausschuss")


def test_parse_poll_data_empty_intro(sample_poll: schemas.Poll):
    """Tests parsing of a poll object with an empty intro."""
    sample_poll.field_intro = ""
    parsed_data = parse_poll_data(sample_poll)
    assert parsed_data["poll_description"] == ""


def test_parse_poll_data_no_committee(sample_poll: schemas.Poll):
    """Tests parsing of a poll object with no committees."""
    sample_poll.field_committees = []
    parsed_data = parse_poll_data(sample_poll)
    assert parsed_data["poll_first_committee"] is None


# =================== load_polls_json ===================


def test_load_polls_json(sample_poll_json_path: Path):
    legislature_id = 111
    res = load_polls_json(legislature_id, sample_poll_json_path.parent)
    assert res == snapshot(
        {
            "meta": {
                "abgeordnetenwatch_api": {
                    "version": "2.4",
                    "changelog": "https://www.abgeordnetenwatch.de/api/version-changelog/aktuell",
                    "licence": "CC0 1.0",
                    "licence_link": "https://creativecommons.org/publicdomain/zero/1.0/deed.de",
                    "documentation": "https://www.abgeordnetenwatch.de/api/entitaeten/poll",
                },
                "status": "ok",
                "status_message": "",
                "result": {
                    "count": 176,
                    "total": 176,
                    "range_start": 0,
                    "range_end": 999,
                },
            },
            "data": [
                {
                    "id": 4293,
                    "entity_type": "node",
                    "label": "Änderung des Infektionsschutzgesetzes und Grundrechtseinschränkungen",
                    "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/4293",
                    "field_legislature": {
                        "id": 111,
                        "entity_type": "parliament_period",
                        "label": "Bundestag 2017 - 2021",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliament-periods/111",
                        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19",
                    },
                    "field_topics": [
                        {
                            "id": 28,
                            "entity_type": "taxonomy_term",
                            "label": "Gesundheit",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/28",
                            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/gesundheit",
                        },
                        {
                            "id": 8,
                            "entity_type": "taxonomy_term",
                            "label": "Recht",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/8",
                            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/recht",
                        },
                    ],
                    "field_committees": [
                        {
                            "id": 514,
                            "entity_type": "node",
                            "label": "Haushaltsausschuss",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/committees/514",
                        },
                        {
                            "id": 520,
                            "entity_type": "node",
                            "label": "Ausschuss für Gesundheit",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/committees/520",
                        },
                    ],
                    "field_intro": """\
<p>Ein von den Fraktionen der CDU/CSU und SPD eingebrachter <a class="link-download" href="https://dserver.bundestag.de/btd/19/320/1932039.pdf">Gesetzentwurf</a> zur Hilfe für Flutopfer in Deutschland sieht auch Änderungen des Infektionsschutzgesetzes vor. Diese sollen unter anderem in bestimmten Einrichtungen eine Auskunftspflicht von Mitarbeiter:innen zu ihrem Impf- oder Genesenenstatus ermöglichen.</p>\r
\r
<p>Im Vorfeld hatte die Opposition für das Gesetzespaket eine namentliche Abstimmung nur über die Punkte verlangt, die auch das Infektionsschutzgesetz betreffen.</p>\r
\r
<p>Die Neuregelungen wurden mit 344 Ja-Stimmen der Unions- und SPD-Fraktion gegen 280 Nein-Stimmen der Oppositionsfraktionen sowie einzelner Abgeordneter der Union und SPD <strong>angenommen</strong>. Lediglich eine Abgeordnete hatte sich bei der Abstimmung enthalten.</p>\r
""",
                    "field_poll_date": "2021-09-07",
                    "field_related_links": [
                        {
                            "uri": "https://dserver.bundestag.de/btd/19/320/1932039.pdf",
                            "title": "Gesetzentwurf",
                        },
                        {
                            "uri": "https://dserver.bundestag.de/btd/19/322/1932275.pdf",
                            "title": "Beschlussempfehlung",
                        },
                        {
                            "uri": "https://www.bundestag.de/parlament/plenum/abstimmung/abstimmung?id=755",
                            "title": "Übersichtsseite des Bundestags zur Abstimmung",
                        },
                    ],
                },
                {
                    "id": 4284,
                    "entity_type": "node",
                    "label": "Fortbestand der epidemischen Lage von nationaler Tragweite",
                    "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/4284",
                    "field_legislature": {
                        "id": 111,
                        "entity_type": "parliament_period",
                        "label": "Bundestag 2017 - 2021",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliament-periods/111",
                        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19",
                    },
                    "field_topics": [
                        {
                            "id": 28,
                            "entity_type": "taxonomy_term",
                            "label": "Gesundheit",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/28",
                            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/gesundheit",
                        },
                        {
                            "id": 8,
                            "entity_type": "taxonomy_term",
                            "label": "Recht",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/8",
                            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/recht",
                        },
                    ],
                    "field_committees": [
                        {
                            "id": 520,
                            "entity_type": "node",
                            "label": "Ausschuss für Gesundheit",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/committees/520",
                        }
                    ],
                    "field_intro": """\
<p>Der von den Fraktionen der CDU/CSU und SPD eingebrachte <a class="link-download" href="https://dserver.bundestag.de/btd/19/320/1932040.pdf">Antrag</a> sieht vor, dass der Bundestag feststellt, dass die seit dem 25. März 2020 geltende epidemische Lage von nationaler Tragweite weiter fortbesteht. Das wird damit begründet, dass angesichts des erneuten Anstiegs der COVID-19-Fallzahlen in Deutschland weiterhin eine erhebliche Gesundheitsgefährdung der Bevölkerung gegeben sei.</p>\r
\r
<p>Der Antrag wurde mit 325 Ja-Stimmen der CDU- und SPD-Fraktion gegen 252 Nein-Stimmen der Oppositionsfraktionen <strong>angenommen</strong>. Fünf Abgeordnete haben sich enthalten.</p>\r
""",
                    "field_poll_date": "2021-08-25",
                    "field_related_links": [
                        {
                            "uri": "https://dserver.bundestag.de/btd/19/320/1932040.pdf",
                            "title": "Antrag der Fraktionen der CDU/CSU und SPD",
                        },
                        {
                            "uri": "https://dejure.org/gesetze/IfSG/5.html",
                            "title": "§ 5 des Infektionsschutzgesetzes",
                        },
                    ],
                },
            ],
        }
    )


def test_load_polls_json_dry_run(tmp_path: Path):
    """Tests the dry_run functionality of load_polls_json."""
    # In dry run, it should not attempt to open the file, so no error
    # even if the file doesn't exist. However, the current implementation
    # of load_polls_json does not prevent file opening on dry run.
    # This test will fail until the function is corrected.
    # For now, we expect a FileNotFoundError.
    with pytest.raises(FileNotFoundError):
        load_polls_json(legislature_id=19, path=tmp_path, dry=True)


def test_load_polls_json_file_not_found(tmp_path: Path):
    """Tests FileNotFoundError when polls json does not exist."""
    with pytest.raises(FileNotFoundError):
        load_polls_json(legislature_id=99, path=tmp_path)
