import json
import typing as T
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from bundestag.data.download.abgeordnetenwatch.cli import get_user_download_decision
from bundestag.data.download.abgeordnetenwatch.download import (
    get_all_remaining_vote_data,
    identify_remaining_poll_ids,
    request_and_store_poll_ids,
    run,
)


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


def test_identify_remaining_poll_ids():
    possible_ids = [1, 2, 3]
    known_ids = {1: Path("dummy"), 2: Path("dummy2")}

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
