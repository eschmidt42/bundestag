import json
import typing as T
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
import requests

from bundestag.data.download.abgeordnetenwatch.cli import get_user_download_decision
from bundestag.data.download.abgeordnetenwatch.download import (
    get_all_remaining_vote_data,
    identify_remaining_poll_ids,
    request_and_store_poll_ids,
    run,
)
from bundestag.data.download.abgeordnetenwatch.request import (
    request_mandates_data,
    request_poll_data,
    request_vote_data,
)
from bundestag.data.download.abgeordnetenwatch.store import (
    check_possible_poll_ids,
    check_stored_vote_ids,
    list_polls_files,
    list_votes_dirs,
    store_mandates_json,
    store_polls_json,
    store_vote_json,
)


@pytest.mark.parametrize(
    "func,dry,status_code",
    [
        (func, dry, status_code)
        for func in [
            request_poll_data,
            request_mandates_data,
            request_vote_data,
        ]
        for dry in [True, False]
        for status_code in [200, 201]
    ],
)
def test_request_data(func: T.Callable, dry: bool, status_code: int):
    r = requests.Response()
    r.status_code = status_code
    r.url = "blub"
    with (
        patch("requests.get", MagicMock(return_value=r)) as _get,
        patch.object(r, "json", MagicMock()),
    ):
        # line to test
        try:
            func(42, dry=dry)
        except AssertionError as ex:
            if status_code != 200:
                pytest.xfail("Not 200 status_code value should raise an exception")
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
        store_polls_json(polls, legislature_id, dry=dry, path=path)

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
        store_mandates_json(polls, legislature_id, dry=dry, path=path)

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
        store_vote_json(votes, poll_id, dry=dry, path=path)

        if dry:
            assert _open.call_count == 0
            assert _mkdir.call_count == 0
        else:
            assert _open.call_count == 1
            assert _mkdir.call_count == 1
            json_dump.assert_called_once()


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
        tmp = list_votes_dirs(path=path)
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
def test_list_polls_files(legislature_id: int, exists: bool, result: T.Dict[int, Path]):
    path = Path("dummy/path")
    glob_leg = (
        [Path(f"dummy/path/votes_legislature_{legislature_id}/poll_11_votes.json")]
        if legislature_id == 21
        else []
    )

    with (
        patch("pathlib.Path.glob", MagicMock(return_value=glob_leg)) as _glob,
        patch("pathlib.Path.exists", MagicMock(return_value=exists)) as _glob2,
    ):
        # line to test
        tmp = list_polls_files(legislature_id, path=path)
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
                    1: Path("dummy/path/votes_legislature_21/poll_1_votes.json"),
                    2: Path("dummy/path/votes_legislature_21/poll_2_votes.json"),
                }
            },
        ),
        # all legislatures
        (
            None,
            {
                21: {
                    1: Path("dummy/path/votes_legislature_21/poll_1_votes.json"),
                    2: Path("dummy/path/votes_legislature_21/poll_2_votes.json"),
                },
                22: {1: Path("dummy/path/votes_legislature_22/poll_1_votes.json")},
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
            "bundestag.data.download.abgeordnetenwatch.store.list_votes_dirs",
            MagicMock(return_value=votes_dirs),
        ) as list_votes_dirs,
        patch(
            "bundestag.data.download.abgeordnetenwatch.store.list_polls_files",
            MagicMock(side_effect=polls_files),
        ) as list_polls_files,
    ):
        # line to test
        tmp = check_stored_vote_ids(legislature_id, path=path)

        assert tmp == result


@pytest.mark.parametrize(
    "choice,result",
    [
        ("y", True),
        ("Y", True),
        ("n", False),
        ("N", False),
        ("wups", None),
        ("", True),
        (None, True),
        (42, None),
    ],
)
def test_get_user_download_decision(choice: str, result: bool):
    with patch("builtins.input", MagicMock(return_value=choice)) as _input:
        try:
            # line to test
            tmp = get_user_download_decision(99, max_tries=1)
        except ValueError:
            if choice == "wups" or choice == 42:
                pytest.xfail("Expected ValueError")
        else:
            assert tmp == result


@pytest.fixture(scope="module")
def poll_response_raw() -> dict:
    response = json.load(open("tests/data_for_testing/polls_legislature_111.json", "r"))
    return response


@pytest.mark.parametrize(
    "dry,result",
    [
        (True, []),
        (
            False,
            [
                4293,
                4284,
            ],
        ),
    ],
)
def test_check_possible_poll_ids(
    dry: bool, result: T.List[int], poll_response_raw: dict
):
    data = poll_response_raw if not dry else {}
    with (
        patch("bundestag.data.utils.load_json", MagicMock(return_value=data)) as _load,
    ):
        # line to test
        tmp = check_possible_poll_ids(42, path=Path("dummy/path"), dry=dry)
        assert set(tmp) == set(result)


def test_identify_remaining_poll_ids():
    possible_ids = [1, 2, 3]
    known_ids = [1, 2]

    # line to test
    tmp = identify_remaining_poll_ids(possible_ids, known_ids)
    assert tmp == [3]


@pytest.mark.skip("mocking broken")
@pytest.mark.parametrize("dry", [True, False])
def test_request_and_store_poll_ids(dry: bool):
    dt_rv_scale = 0.1
    remaining_poll_ids = [1, 2, 3]
    t_sleep = 1
    path = Path("dummy/path")

    with (
        patch(
            "bundestag.data.download.abgeordnetenwatch.request.request_vote_data",
            MagicMock(return_value={}),
        ) as _request,
        patch(
            "bundestag.data.download.abgeordnetenwatch.store.store_vote_json",
            MagicMock(),
        ) as _store,
        patch("time.sleep", MagicMock()) as _sleep,
    ):
        # line to test
        request_and_store_poll_ids(dt_rv_scale, remaining_poll_ids, dry, t_sleep, path)

        if dry:
            assert _sleep.call_count == 0
        else:
            assert _sleep.call_count == len(remaining_poll_ids)

        _request.assert_has_calls(
            [call(1, dry=dry), call(2, dry=dry), call(3, dry=dry)]
        )
        _store.assert_has_calls(
            [
                call({}, 1, dry=dry, path=path),
                call({}, 2, dry=dry, path=path),
                call({}, 3, dry=dry, path=path),
            ]
        )


@pytest.mark.skip("mocking broken")
@pytest.mark.parametrize(
    "dry,ask_user,remaining_poll_ids,decision",
    [
        (dry, ask_user, remaining_poll_ids, decision)
        for dry in [True, False]
        for ask_user in [True, False]
        for remaining_poll_ids in [[], [1, 2, 3]]
        for decision in [True, False]
    ],
)
def test_get_all_remaining_vote_data(
    dry: bool, ask_user: bool, remaining_poll_ids: T.List[int], decision: bool
):
    dt_rv_scale = 0.1
    t_sleep = 1
    path = Path("dummy/path")
    legislature_id = 42

    with (
        patch(
            "bundestag.data.download.abgeordnetenwatch.store.check_stored_vote_ids",
            MagicMock(),
        ) as _check_stored,
        patch(
            "bundestag.data.download.abgeordnetenwatch.store.check_possible_poll_ids",
            MagicMock(),
        ) as _check_possible,
        patch(
            "bundestag.data.download.abgeordnetenwatch.download.identify_remaining_poll_ids",
            MagicMock(return_value=remaining_poll_ids),
        ) as _identify_remaining,
        patch(
            "bundestag.data.download.abgeordnetenwatch.cli.get_user_download_decision",
            MagicMock(return_value=decision),
        ) as _get_user_decision,
        patch(
            "bundestag.data.download.abgeordnetenwatch.download.request_and_store_poll_ids",
            MagicMock(),
        ) as _request_and_store,
    ):
        # line to test
        get_all_remaining_vote_data(
            legislature_id=legislature_id,
            dt_rv_scale=dt_rv_scale,
            dry=dry,
            t_sleep=t_sleep,
            path=path,
            ask_user=ask_user,
        )

        _check_stored.assert_called_once_with(legislature_id=legislature_id, path=path)
        _check_possible.assert_called_once_with(
            legislature_id=legislature_id, path=path, dry=dry
        )
        _identify_remaining.assert_called_once()

        if len(remaining_poll_ids) > 0:
            if ask_user:
                _get_user_decision.assert_called_once_with(len(remaining_poll_ids))
            else:
                _get_user_decision.assert_not_called()

            if ask_user and not decision:
                _request_and_store.assert_not_called()
            else:
                _request_and_store.assert_called_once_with(
                    dt_rv_scale, remaining_poll_ids, dry, t_sleep, path
                )

        else:
            _get_user_decision.assert_not_called()
            _request_and_store.assert_not_called()


@pytest.mark.skip("mocking broken")
@pytest.mark.parametrize(
    "dry,path_exists,raw_path,assume_yes",
    [
        (dry, path_exists, raw_path, assume_yes)
        for dry in [True, False]
        for path_exists in [True, False]
        for raw_path in [Path("dummy/path"), None]
        for assume_yes in [True, False]
    ],
)
def test_run(dry: bool, path_exists: bool, raw_path: Path, assume_yes: bool):
    legislature_id = 42
    max_polls = 42
    max_mandates = 21
    t_sleep = 1
    dt_rv_scale = 0.1
    ask_user = True

    with (
        patch("pathlib.Path.exists", MagicMock(return_value=path_exists)) as _exists,
        patch("bundestag.data.utils.ensure_path_exists", MagicMock()) as _ensure_exists,
        patch(
            "bundestag.data.download.abgeordnetenwatch.request_poll_data",
            MagicMock(return_value={}),
        ) as _request_poll_data,
        patch(
            "bundestag.data.download.abgeordnetenwatch.store_polls_json",
            MagicMock(),
        ) as _store_polls_json,
        patch(
            "bundestag.data.download.abgeordnetenwatch.request_mandates_data",
            MagicMock(return_value={}),
        ) as _request_mandates_data,
        patch(
            "bundestag.data.download.abgeordnetenwatch.store_mandates_json",
            MagicMock(),
        ) as _store_mandates_json,
        patch(
            "bundestag.data.download.abgeordnetenwatch.get_all_remaining_vote_data",
            MagicMock(),
        ) as _remaining_vote_data,
    ):
        try:
            # line to test
            run(
                legislature_id=legislature_id,
                dt_rv_scale=dt_rv_scale,
                dry=dry,
                t_sleep=t_sleep,
                raw_path=raw_path,
                ask_user=ask_user,
                max_mandates=max_mandates,
                max_polls=max_polls,
                assume_yes=assume_yes,
            )
        except ValueError:
            if raw_path is None and not dry:
                pytest.xfail("raw_path is None and not dry has to fail")
        else:
            if not dry:
                _exists.assert_called_once()

            if not path_exists and not dry:
                _ensure_exists.assert_called_once_with(raw_path, assume_yes=assume_yes)

            _request_poll_data.assert_called_once_with(
                legislature_id, dry=dry, num_polls=max_polls
            )
            _store_polls_json.assert_called_once_with(
                {}, legislature_id, dry=dry, path=raw_path
            )
            _request_mandates_data.assert_called_once_with(
                legislature_id, dry=dry, num_mandates=max_mandates
            )
            _store_mandates_json.assert_called_once_with(
                {}, legislature_id, dry=dry, path=raw_path
            )
            _remaining_vote_data.assert_called_once_with(
                legislature_id,
                dry=dry,
                t_sleep=t_sleep,
                dt_rv_scale=dt_rv_scale,
                path=raw_path,
                ask_user=ask_user,
            )
