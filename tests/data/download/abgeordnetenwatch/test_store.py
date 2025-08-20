import json
from pathlib import Path

import pytest

from bundestag.data.download.abgeordnetenwatch.store import (
    check_possible_poll_ids,
    check_stored_vote_ids,
    list_polls_files,
    list_votes_dirs,
    store_mandates_json,
    store_polls_json,
    store_vote_json,
)


def test_store_polls_json(tmp_path: Path):
    """Test storing polls data to a JSON file."""
    # Setup
    legislature_id = 19
    polls_data = {"poll1": "data1", "poll2": "data2"}

    # Execute
    store_polls_json(tmp_path, polls_data, legislature_id, dry=False)

    # Verify
    expected_file = tmp_path / f"polls_legislature_{legislature_id}.json"
    assert expected_file.exists()
    with open(expected_file, "r", encoding="utf8") as f:
        content = json.load(f)
    assert content == polls_data


def test_store_polls_json_dry_run(tmp_path: Path):
    """Test storing polls data in dry run mode."""
    # Setup
    legislature_id = 19
    polls_data = {"poll1": "data1", "poll2": "data2"}

    # Execute
    store_polls_json(tmp_path, polls_data, legislature_id, dry=True)

    # Verify
    expected_file = tmp_path / f"polls_legislature_{legislature_id}.json"
    assert not expected_file.exists()


def test_store_polls_json_with_none(tmp_path: Path):
    """Test storing None as polls data."""
    # Setup
    legislature_id = 19
    polls_data = None

    # Execute
    store_polls_json(tmp_path, polls_data, legislature_id, dry=False)

    # Verify
    expected_file = tmp_path / f"polls_legislature_{legislature_id}.json"
    assert expected_file.exists()
    with open(expected_file, "r", encoding="utf8") as f:
        content = json.load(f)
    assert content is None


def test_store_mandates_json(tmp_path: Path):
    """Test storing mandates data to a JSON file."""
    # Setup
    legislature_id = 19
    mandates_data = {"mandate1": "data1", "mandate2": "data2"}

    # Execute
    store_mandates_json(tmp_path, mandates_data, legislature_id, dry=False)

    # Verify
    expected_file = tmp_path / f"mandates_legislature_{legislature_id}.json"
    assert expected_file.exists()
    with open(expected_file, "r", encoding="utf8") as f:
        content = json.load(f)
    assert content == mandates_data


def test_store_mandates_json_dry_run(tmp_path: Path):
    """Test storing mandates data in dry run mode."""
    # Setup
    legislature_id = 19
    mandates_data = {"mandate1": "data1", "mandate2": "data2"}

    # Execute
    store_mandates_json(tmp_path, mandates_data, legislature_id, dry=True)

    # Verify
    expected_file = tmp_path / f"mandates_legislature_{legislature_id}.json"
    assert not expected_file.exists()


def test_store_mandates_json_with_none(tmp_path: Path):
    """Test storing None as mandates data."""
    # Setup
    legislature_id = 19
    mandates_data = None

    # Execute
    store_mandates_json(tmp_path, mandates_data, legislature_id, dry=False)

    # Verify
    expected_file = tmp_path / f"mandates_legislature_{legislature_id}.json"
    assert expected_file.exists()
    with open(expected_file, "r", encoding="utf8") as f:
        content = json.load(f)
    assert content is None


def test_store_vote_json(tmp_path: Path):
    """Test storing vote data to a JSON file."""
    # Setup
    legislature_id = 19
    poll_id = 123
    votes_data = {"data": {"field_legislature": {"id": legislature_id}}}

    # Execute
    store_vote_json(tmp_path, votes_data, poll_id, dry=False)

    # Verify
    expected_file = (
        tmp_path / f"votes_legislature_{legislature_id}" / f"poll_{poll_id}_votes.json"
    )
    assert expected_file.exists()
    with open(expected_file, "r", encoding="utf8") as f:
        content = json.load(f)
    assert content == votes_data


def test_store_vote_json_dry_run(tmp_path: Path):
    """Test storing vote data in dry run mode."""
    # Setup
    legislature_id = 19
    poll_id = 123
    votes_data = {"data": {"field_legislature": {"id": legislature_id}}}

    # Execute
    store_vote_json(tmp_path, votes_data, poll_id, dry=True)

    # Verify
    expected_file = (
        tmp_path / f"votes_legislature_{legislature_id}" / f"poll_{poll_id}_votes.json"
    )
    assert not expected_file.exists()
    assert not expected_file.parent.exists()


def test_store_vote_json_with_none_raises_error(tmp_path: Path):
    """Test that storing None as vote data raises a ValueError."""
    # Setup
    poll_id = 123
    votes_data = None

    # Execute & Verify
    with pytest.raises(ValueError):
        store_vote_json(tmp_path, votes_data, poll_id, dry=False)


def test_list_votes_dirs(tmp_path: Path):
    """Test listing vote directories."""
    # Setup
    (tmp_path / "votes_legislature_19").mkdir()
    (tmp_path / "votes_legislature_20").mkdir()
    (tmp_path / "other_dir").mkdir()

    # Execute
    result = list_votes_dirs(tmp_path)

    # Verify
    assert len(result) == 2
    assert 19 in result
    assert 20 in result
    assert result[19] == tmp_path / "votes_legislature_19"
    assert result[20] == tmp_path / "votes_legislature_20"


def test_list_votes_dirs_no_dirs(tmp_path: Path):
    """Test listing vote directories when none exist."""
    # Setup
    (tmp_path / "other_dir").mkdir()

    # Execute
    result = list_votes_dirs(tmp_path)

    # Verify
    assert result == {}


def test_list_votes_dirs_empty_dir(tmp_path: Path):
    """Test listing vote directories in an empty directory."""
    # Execute
    result = list_votes_dirs(tmp_path)

    # Verify
    assert result == {}


def test_list_polls_files(tmp_path: Path):
    """Test listing poll files."""
    # Setup
    legislature_id = 19
    leg_path = tmp_path / f"votes_legislature_{legislature_id}"
    leg_path.mkdir()
    (leg_path / "poll_123_votes.json").touch()
    (leg_path / "poll_456_votes.json").touch()
    (leg_path / "other_file.txt").touch()

    # Execute
    result = list_polls_files(legislature_id, tmp_path)

    # Verify
    assert len(result) == 2
    assert 123 in result
    assert 456 in result
    assert result[123] == leg_path / "poll_123_votes.json"
    assert result[456] == leg_path / "poll_456_votes.json"


def test_list_polls_files_no_files(tmp_path: Path):
    """Test listing poll files when none exist."""
    # Setup
    legislature_id = 19
    leg_path = tmp_path / f"votes_legislature_{legislature_id}"
    leg_path.mkdir()
    (leg_path / "other_file.txt").touch()

    # Execute
    result = list_polls_files(legislature_id, tmp_path)

    # Verify
    assert result == {}


def test_list_polls_files_no_dir(tmp_path: Path):
    """Test listing poll files when the directory does not exist."""
    # Setup
    legislature_id = 19

    # Execute
    result = list_polls_files(legislature_id, tmp_path)

    # Verify
    assert result == {}


def test_check_stored_vote_ids_known_legislature(tmp_path: Path):
    """Test checking stored vote IDs for a known legislature."""
    # Setup
    legislature_id = 19
    leg_path = tmp_path / f"votes_legislature_{legislature_id}"
    leg_path.mkdir()
    (leg_path / "poll_123_votes.json").touch()

    # Execute
    result = check_stored_vote_ids(legislature_id, tmp_path)

    # Verify
    assert legislature_id in result
    assert 123 in result[legislature_id]
    assert result[legislature_id][123] == leg_path / "poll_123_votes.json"


def test_check_stored_vote_ids_unknown_legislature(tmp_path: Path):
    """Test checking stored vote IDs for an unknown legislature."""
    # Setup
    legislature_id = 20
    (tmp_path / "votes_legislature_19").mkdir()

    # Execute
    result = check_stored_vote_ids(legislature_id, tmp_path)

    # Verify
    assert result == {legislature_id: {}}


def test_check_stored_vote_ids_all_legislatures(tmp_path: Path):
    """Test checking stored vote IDs for all legislatures."""
    # Setup
    leg_path_19 = tmp_path / "votes_legislature_19"
    leg_path_19.mkdir()
    (leg_path_19 / "poll_1_votes.json").touch()

    leg_path_20 = tmp_path / "votes_legislature_20"
    leg_path_20.mkdir()
    (leg_path_20 / "poll_2_votes.json").touch()

    # Execute
    result = check_stored_vote_ids(None, tmp_path)

    # Verify
    assert 19 in result
    assert 20 in result
    assert 1 in result[19]
    assert 2 in result[20]
    assert result[19][1] == leg_path_19 / "poll_1_votes.json"
    assert result[20][2] == leg_path_20 / "poll_2_votes.json"


def test_check_possible_poll_ids(tmp_path: Path):
    """Test checking possible poll IDs."""
    # Setup
    legislature_id = 19
    polls_file = tmp_path / f"polls_legislature_{legislature_id}.json"
    polls_data = {
        "meta": {
            "abgeordnetenwatch_api": {
                "version": "v1",
                "changelog": "",
                "licence": "",
                "licence_link": "",
                "documentation": "",
            },
            "status": "ok",
            "status_message": "",
            "result": {"count": 3, "total": 3, "range_start": 0, "range_end": 3},
        },
        "data": [
            {
                "id": 1,
                "entity_type": "poll",
                "label": "Poll 1",
                "api_url": "api/v1/poll/1",
                "field_legislature": {
                    "id": 19,
                    "entity_type": "legislature",
                    "label": "19. Wahlperiode",
                    "api_url": "api/v1/legislature/19",
                    "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19-wahlperiode",
                },
                "field_intro": "Intro 1",
                "field_poll_date": "2025-08-20",
            },
            {
                "id": 2,
                "entity_type": "poll",
                "label": "Poll 2",
                "api_url": "api/v1/poll/2",
                "field_legislature": {
                    "id": 19,
                    "entity_type": "legislature",
                    "label": "19. Wahlperiode",
                    "api_url": "api/v1/legislature/19",
                    "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19-wahlperiode",
                },
                "field_intro": "Intro 2",
                "field_poll_date": "2025-08-21",
            },
            {
                "id": 3,
                "entity_type": "poll",
                "label": "Poll 3",
                "api_url": "api/v1/poll/3",
                "field_legislature": {
                    "id": 19,
                    "entity_type": "legislature",
                    "label": "19. Wahlperiode",
                    "api_url": "api/v1/legislature/19",
                    "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/bundestag/19-wahlperiode",
                },
                "field_intro": "Intro 3",
                "field_poll_date": "2025-08-22",
            },
        ],
    }
    with open(polls_file, "w") as f:
        json.dump(polls_data, f)

    # Execute
    result = check_possible_poll_ids(legislature_id, tmp_path)

    # Verify
    assert sorted(result) == [1, 2, 3]


def test_check_possible_poll_ids_dry_run(tmp_path: Path):
    """Test checking possible poll IDs in dry run mode."""
    # Setup
    legislature_id = 19
    polls_file = tmp_path / f"polls_legislature_{legislature_id}.json"
    polls_file.touch()  # File exists but is not read in dry run

    # Execute
    result = check_possible_poll_ids(legislature_id, tmp_path, dry=True)

    # Verify
    assert result == []


def test_check_possible_poll_ids_file_not_found(tmp_path: Path):
    """Test checking possible poll IDs when the file does not exist."""
    # Setup
    legislature_id = 19

    # Execute & Verify
    with pytest.raises(FileNotFoundError):
        check_possible_poll_ids(legislature_id, tmp_path)
