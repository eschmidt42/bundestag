import typing as T
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

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


@pytest.mark.parametrize(
    "legislature_id,result",
    [
        (42, {}),
        (21, {21: Path("dummy/path/votes_legislature_21")}),
    ],
)
def test_list_votes_dirs(legislature_id: int, result: T.Dict[int, Path]):
    path = Path("dummy/path")
    glob_leg = (
        [Path(f"dummy/path/votes_legislature_{legislature_id}")]
        if legislature_id == 21
        else []
    )

    with patch("pathlib.Path.glob", MagicMock(return_value=glob_leg)) as _glob:
        # line to test
        tmp = aw.list_votes_dirs(path=path)
        assert tmp == result


@pytest.mark.parametrize(
    "legislature_id,exists,result",
    [
        (42, False, {}),
        (
            21,
            True,
            {11: Path("dummy/path/votes_legislature_21/poll_11_votes.json")},
        ),
    ],
)
def test_list_polls_files(
    legislature_id: int, exists: bool, result: T.Dict[int, Path]
):
    path = Path("dummy/path")
    glob_leg = (
        [
            Path(
                f"dummy/path/votes_legislature_{legislature_id}/poll_11_votes.json"
            )
        ]
        if legislature_id == 21
        else []
    )

    with (
        patch("pathlib.Path.glob", MagicMock(return_value=glob_leg)) as _glob,
        patch("pathlib.Path.exists", MagicMock(return_value=exists)) as _glob2,
    ):
        # line to test
        tmp = aw.list_polls_files(legislature_id, path=path)
        assert tmp == result


@pytest.mark.parametrize(
    "legislature_id,result",
    [
        # unknown legislature
        (42, {42: {}}),
        # known legislature
        (
            21,
            {
                21: {
                    1: Path(
                        "dummy/path/votes_legislature_21/poll_1_votes.json"
                    ),
                    2: Path(
                        "dummy/path/votes_legislature_21/poll_2_votes.json"
                    ),
                }
            },
        ),
        # all legislatures
        (
            None,
            {
                21: {
                    1: Path(
                        "dummy/path/votes_legislature_21/poll_1_votes.json"
                    ),
                    2: Path(
                        "dummy/path/votes_legislature_21/poll_2_votes.json"
                    ),
                },
                22: {
                    1: Path(
                        "dummy/path/votes_legislature_22/poll_1_votes.json"
                    )
                },
            },
        ),
    ],
)
def test_check_stored_vote_ids(
    legislature_id: int, result: T.Dict[int, T.Dict[int, Path]]
):
    path = Path("dummy/path")

    if legislature_id == 42:
        votes_dirs = {}
        polls_files = [{}]
    elif legislature_id == 21:
        votes_dirs = {21: Path(f"dummy/path/votes_legislature_21")}
        polls_files = [
            {
                1: Path("dummy/path/votes_legislature_21/poll_1_votes.json"),
                2: Path("dummy/path/votes_legislature_21/poll_2_votes.json"),
            }
        ]
    elif legislature_id == None:
        votes_dirs = {
            21: Path(f"dummy/path/votes_legislature_21"),
            22: Path(f"dummy/path/votes_legislature_22"),
        }
        polls_files = [
            {
                1: Path("dummy/path/votes_legislature_21/poll_1_votes.json"),
                2: Path("dummy/path/votes_legislature_21/poll_2_votes.json"),
            },
            {1: Path("dummy/path/votes_legislature_22/poll_1_votes.json")},
        ]
    else:
        raise ValueError("Invalid `legislature_id`")

    with (
        patch(
            "bundestag.data.download.abgeordnetenwatch.list_votes_dirs",
            MagicMock(return_value=votes_dirs),
        ) as list_votes_dirs,
        patch(
            "bundestag.data.download.abgeordnetenwatch.list_polls_files",
            MagicMock(side_effect=polls_files),
        ) as list_polls_files,
    ):
        # line to test
        tmp = aw.check_stored_vote_ids(legislature_id, path=path)

        assert tmp == result


# def test_stored_vote_ids_check(path: Path = None):
#     tmp = check_stored_vote_ids(path=path)
#     assert isinstance(tmp, dict), "Sanity check for dict type of `tmp` failed"
#     assert all(
#         [isinstance(v, dict) for v in tmp.values()]
#     ), "Sanity check for dict type of values of `tmp` failed"
#     assert all(
#         [isinstance(p, Path) for d in tmp.values() for p in d.values()]
#     ), "Sanity check of lowest level values failed, expect all to be of type pathlib.Path"
@pytest.mark.parametrize(
    "choice,result",
    [
        ("y", True),
        ("Y", True),
        ("n", False),
        ("N", False),
        ("wups", None),
    ],
)
def test_get_user_download_decision(choice: str, result: bool):
    with patch("builtins.input", MagicMock(return_value=choice)) as _input:
        try:
            # line to test
            tmp = aw.get_user_download_decision(99, max_tries=1)
        except ValueError:
            if choice == "wups":
                pytest.xfail("Expected ValueError")
        else:
            assert tmp == result


@pytest.mark.skip("to be implemented")
def test_check_possible_poll_ids():
    ...


@pytest.mark.skip("to be implemented")
def test_get_all_remaining_vote_data():
    ...


@pytest.mark.skip("to be implemented")
def test_run():
    ...
