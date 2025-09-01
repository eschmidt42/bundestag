from pathlib import Path

import polars as pl
import pytest
from inline_snapshot import snapshot

from bundestag.data.transform.abgeordnetenwatch.transform import (
    get_mandates_parquet_path,
    get_polls_parquet_path,
    get_votes_csv_path,
    get_votes_parquet_path,
    run,
    transform_mandates_data,
    transform_votes_data,
)


@pytest.fixture(scope="module")
def raw_path() -> Path:
    return Path("tests/data_for_testing")


@pytest.fixture
def MANDATES_DF() -> pl.DataFrame:
    return pl.from_dicts(
        [
            {
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
            },
            {
                "legislature_id": 111,
                "legislature_period": "Bundestag 2017 - 2021",
                "mandate_id": 52107,
                "mandate": "Florian Jäger (Bundestag 2017 - 2021)",
                "politician_id": 121214,
                "politician": "Florian Jäger",
                "politician_url": "https://www.abgeordnetenwatch.de/profile/florian-jaeger",
                "start_date": "2021-07-20",
                "end_date": "",
                "constituency_id": 4339,
                "constituency_name": "215 - Fürstenfeldbruck (Bundestag 2017 - 2021)",
                "fraction_names": ["AfD seit 20.07.2021"],
                "fraction_ids": [9228],
                "fraction_starts": ["2021-07-20"],
                "fraction_ends": [""],
            },
        ]
    )


@pytest.fixture
def VOTES_DF() -> pl.DataFrame:
    return pl.from_dict(
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


def test_transform_mandates_data(MANDATES_DF: pl.DataFrame):
    res = transform_mandates_data(MANDATES_DF)
    assert "all_parties" in res.columns
    assert res["all_parties"].to_list() == snapshot([["DIE LINKE"], ["AfD"]])
    assert "party" in res.columns
    assert res["party"].to_list() == snapshot(["DIE LINKE", "AfD"])


def test_transform_votes_data(VOTES_DF: pl.DataFrame):
    res = transform_votes_data(VOTES_DF)
    assert "politician name" in res.columns
    assert res.drop(["politician name"]).equals(VOTES_DF)
    assert res["politician name"].to_list() == snapshot(
        ["Michael von Abercron", "Stephan Albani"]
    )


@pytest.mark.parametrize(
    "dry,validate",
    [
        (
            dry,
            validate,
        )
        for dry in [True, False]
        for validate in [True, False]
    ],
)
def test_run(
    dry: bool,
    validate: bool,
    raw_path: Path,
    tmp_path: Path,
):
    preprocessed_path = tmp_path / "preprocessed"
    legislature_id = 111
    assume_yes = True
    run(
        legislature_id,
        raw_path,
        preprocessed_path,
        dry,
        validate=validate,
        assume_yes=assume_yes,
    )

    if dry:
        assert not preprocessed_path.exists()
    else:
        assert preprocessed_path.exists()
        votes_parquet_path = get_votes_parquet_path(legislature_id, preprocessed_path)
        votes_csv_path = get_votes_csv_path(legislature_id, preprocessed_path)
        mandates_parquet_path = get_mandates_parquet_path(
            legislature_id, preprocessed_path
        )
        polls_parquet_path = get_polls_parquet_path(legislature_id, preprocessed_path)

        assert votes_parquet_path.exists()
        assert votes_csv_path.exists()
        assert mandates_parquet_path.exists()
        assert polls_parquet_path.exists()
