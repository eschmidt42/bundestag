import json
import typing as T
import unittest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pandera as pa
import pytest
import requests

import bundestag.data.download.abgeordnetenwatch as aw
import bundestag.schemas as schemas


@pytest.mark.parametrize(
    "func,dry,status_code",
    [
        (func, dry, status_code)
        for func in [
            aw.request_poll_data,
            aw.request_mandates_data,
            aw.request_vote_data,
        ]
        for dry in [True, False]
        for status_code in [200, 201]
    ],
)
def test_request_data(func: T.Callable, dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with patch(
        "requests.get", MagicMock(return_value=r)
    ) as _get, patch.object(r, "json", MagicMock()):
        # line to test
        try:
            func(42, dry=dry)
        except AssertionError as ex:
            if status_code != 200:
                pytest.xfail(
                    "Not 200 status_code value should raise an exception"
                )
            else:
                raise ex

        if dry:
            assert _get.call_count == 0
        else:
            assert _get.call_count == 1


@pytest.mark.parametrize("dry", [True, False])
def test_store_polls_json(dry: bool):
    polls = {}
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
    ):
        # line to test
        aw.store_polls_json(polls, legislature_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _mkdir.call_count == 0
            assert _open.call_count == 1
            assert _open.return_value.__enter__().write.call_count == 1


@pytest.mark.parametrize("dry", [True, False])
def test_store_mandates_json(dry: bool):
    polls = {}
    legislature_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_mandates_json(polls, legislature_id, dry=dry, path=path)

        assert _mkdir.call_count == 0
        if dry:
            assert _open.call_count == 0
        else:
            assert _open.call_count == 1
            json_dump.assert_called_once()


@pytest.mark.parametrize("dry", [True, False])
def test_store_vote_json(dry: bool):
    votes = {"data": {"field_legislature": {"id": 21}}}
    poll_id = 42
    path = Path("file/path")

    with (
        patch("pathlib.Path.mkdir", MagicMock()) as _mkdir,
        patch("builtins.open", new_callable=mock_open()) as _open,
        patch("json.dump", MagicMock()) as json_dump,
    ):
        # line to test
        aw.store_vote_json(votes, poll_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _open.call_count == 1
            assert _mkdir.call_count == 1
            json_dump.assert_called_once()


# def test_vote_data(df):
#     "Basic sanity check on vote data"

#     # there should be no missing values for any column in `cols`
#     cols = ["mandate_id", "mandate", "poll_id", "vote"]
#     for c in cols:
#         msg = f"{c}: failed because NaNs/None values were found."
#         mask = df[c].isna()
#         assert mask.sum() == 0, f"{msg}: \n{df.loc[mask].head()}"

#     # there should only be one poll_id value
#     ids = df["poll_id"].unique()
#     msg = f"Surprisingly found multiple poll_id values: {ids}"
#     assert len(ids) == 1, msg

#     # there should be no duplicate mandate_id value
#     mask = df["mandate_id"].duplicated()
#     assert (
#         mask.sum() == 0
#     ), f'Surprisingly found duplicated mandate_id values: {df.loc[mask,"poll_id"].unique()} \nexamples: \n{df.loc[mask].head()}'


@pytest.mark.skip("to be implemented")
def test_check_stored_vote_ids():
    pass


# def test_stored_vote_ids_check(path: Path = None):
#     tmp = check_stored_vote_ids(path=path)
#     assert isinstance(tmp, dict), "Sanity check for dict type of `tmp` failed"
#     assert all(
#         [isinstance(v, dict) for v in tmp.values()]
#     ), "Sanity check for dict type of values of `tmp` failed"
#     assert all(
#         [isinstance(p, Path) for d in tmp.values() for p in d.values()]
#     ), "Sanity check of lowest level values failed, expect all to be of type pathlib.Path"
@pytest.mark.skip("to be implemented")
def test_get_all_remaining_vote_info():
    pass
