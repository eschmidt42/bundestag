from pathlib import Path

import pandas as pd
import pytest

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
def MANDATES_DF() -> pd.DataFrame:
    return pd.DataFrame(
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


@pytest.fixture
def VOTES_DF() -> pd.DataFrame:
    return pd.DataFrame(
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


def test_transform_mandates_data(MANDATES_DF: pd.DataFrame):
    # line to test
    res = transform_mandates_data(MANDATES_DF)
    assert "all_parties" in res.columns
    assert "party" in res.columns


def test_transform_votes_data(VOTES_DF: pd.DataFrame):
    # line to test
    res = transform_votes_data(VOTES_DF)
    assert "politician name" in res.columns
    assert res.drop(columns=["politician name"]).equals(VOTES_DF)


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
