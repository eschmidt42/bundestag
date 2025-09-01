from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

import bundestag.schemas as schemas
from bundestag.data.transform.abgeordnetenwatch.process.votes import (
    compile_votes_data,
    get_votes_data,
    load_vote_json,
    parse_vote_data,
)


def test_get_votes_data(sample_vote_json_path: Path, VOTES_DF: pl.DataFrame):
    legislature_id = 111
    poll_id = 4217
    res = get_votes_data(legislature_id, poll_id, sample_vote_json_path.parent.parent)
    assert res.equals(VOTES_DF)


def test_parse_vote_data(
    sample_votes_response: schemas.VoteResponse, VOTE_DATA_PARSED: dict
):
    data = sample_votes_response.data.related_data.votes[0]
    res = parse_vote_data(data)

    assert all([k in res for k in VOTE_DATA_PARSED])
    for k in VOTE_DATA_PARSED.keys():
        assert VOTE_DATA_PARSED[k] == res[k]


def test_load_vote_json(sample_vote_json_path: Path):
    legislature_id = 111
    poll_id = 4217
    res = load_vote_json(legislature_id, poll_id, sample_vote_json_path.parent.parent)
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
                "result": {"entity_id": "4217", "entity_type": "node"},
            },
            "data": {
                "id": 4217,
                "entity_type": "node",
                "label": "Änderung im Infektions\xadschutz\xadgesetz",
                "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/4217",
                "field_legislature": {
                    "id": 111,
                    "entity_type": "parliament_period",
                    "label": "Bundestag 2017 - 2021",
                    "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliament-periods/111",
                    "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19",
                },
                "field_topics": [
                    {
                        "id": 16,
                        "entity_type": "taxonomy_term",
                        "label": "Gesellschaftspolitik, soziale Gruppen",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/topics/16",
                        "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/themen-dip21/gesellschaftspolitik-soziale-gruppen",
                    },
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
                        "id": 512,
                        "entity_type": "node",
                        "label": "Ausschuss für Recht und Verbraucherschutz",
                        "api_url": "https://www.abgeordnetenwatch.de/api/v2/committees/512",
                    }
                ],
                "field_intro": """\
<p>Abgestimmt wurde über die Paragraphen&nbsp;9&nbsp;und 10 des Infektionsschutzgesetzes. Die AfD hatte verlangt, über einzelne Teile des Gesetzentwurfs und den Gesetzentwurf insgesamt, getrennt abzustimmen. Eine namentlicher Abstimmung fand lediglich bezüglich der Änderungen des Infektionsschutzgesetzes statt.</p>\r
\r
<p>Der Gesetzentwurf wird mit 408 Ja-Stimmen der Fraktionen CDU/CSU, SPD und Bündnis 90/Die Grünen angenommen. Dagegen stimmten die FDP, Die Linke und die AfD.</p>\r
""",
                "field_poll_date": "2021-06-24",
                "field_related_links": [
                    {
                        "uri": "https://dserver.bundestag.de/btd/19/281/1928173.pdf",
                        "title": "Gesetzentwurf",
                    },
                    {
                        "uri": "https://dserver.bundestag.de/btd/19/309/1930938.pdf",
                        "title": "Beschlussempfehlung",
                    },
                    {
                        "uri": "https://dserver.bundestag.de/btd/19/311/1931118.pdf",
                        "title": "Bericht",
                    },
                ],
                "related_data": {
                    "votes": [
                        {
                            "id": 415259,
                            "entity_type": "vote",
                            "label": "Michael von Abercron - Änderung im Infektions\xadschutz\xadgesetz",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/votes/415259",
                            "mandate": {
                                "id": 45467,
                                "entity_type": "candidacy_mandate",
                                "label": "Michael von Abercron (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/45467",
                            },
                            "poll": {
                                "id": 4217,
                                "entity_type": "node",
                                "label": "Änderung im Infektions\xadschutz\xadgesetz",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/4217",
                            },
                            "vote": "yes",
                            "reason_no_show": None,
                            "reason_no_show_other": None,
                            "fraction": {
                                "id": 81,
                                "entity_type": "fraction",
                                "label": "CDU/CSU (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/81",
                            },
                        },
                        {
                            "id": 415260,
                            "entity_type": "vote",
                            "label": "Stephan Albani - Änderung im Infektions\xadschutz\xadgesetz",
                            "api_url": "https://www.abgeordnetenwatch.de/api/v2/votes/415260",
                            "mandate": {
                                "id": 44472,
                                "entity_type": "candidacy_mandate",
                                "label": "Stephan Albani (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/44472",
                            },
                            "poll": {
                                "id": 4217,
                                "entity_type": "node",
                                "label": "Änderung im Infektions\xadschutz\xadgesetz",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/polls/4217",
                            },
                            "vote": "yes",
                            "reason_no_show": None,
                            "reason_no_show_other": None,
                            "fraction": {
                                "id": 81,
                                "entity_type": "fraction",
                                "label": "CDU/CSU (Bundestag 2017 - 2021)",
                                "api_url": "https://www.abgeordnetenwatch.de/api/v2/fractions/81",
                            },
                        },
                    ]
                },
            },
        }
    )


@pytest.mark.parametrize("validate", [True, False])
def test_compile_votes_data(
    sample_vote_json_path: Path, VOTES_DF: pl.DataFrame, validate: bool
):
    legislature_id = 111

    res = compile_votes_data(
        legislature_id, sample_vote_json_path.parent.parent, validate=validate
    )
    assert res.equals(VOTES_DF)
