from pathlib import Path
from unittest.mock import patch

from bundestag.data.download.abgeordnetenwatch import download
from bundestag.data.download.abgeordnetenwatch.download import (
    identify_remaining_poll_ids,
)


def test_identify_remaining_poll_ids():
    possible_ids = [1, 2, 3]
    known_ids = {1: Path("dummy"), 2: Path("dummy2")}

    # line to test
    tmp = identify_remaining_poll_ids(possible_ids, known_ids)
    assert tmp == [3]


@patch("httpx.get")
def test_request_and_store_poll_ids_dry_run(mock_get):
    """Test request_and_store_poll_ids in dry run mode."""
    remaining_poll_ids = [1, 2, 3]
    path = Path("/fake/path")

    download.request_and_store_poll_ids(
        dt_rv_scale=0.1,
        remaining_poll_ids=remaining_poll_ids,
        dry=True,
        t_sleep=1,
        path=path,
    )

    mock_get.assert_not_called()


@patch("bundestag.data.download.abgeordnetenwatch.request.httpx.get")
def test_run_dry(mock_get):
    download.run(legislature_id=20, dry=True, ask_user=False)
    mock_get.assert_not_called()
