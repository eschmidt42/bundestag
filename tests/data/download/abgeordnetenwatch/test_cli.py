from unittest.mock import patch

import pytest

from bundestag.data.download.abgeordnetenwatch import cli


@pytest.mark.parametrize(
    "user_input, expected",
    [
        ("y", True),
        ("Y", True),
        ("n", False),
        ("N", False),
        ("", True),
    ],
)
def test_get_user_download_decision_valid_inputs(user_input, expected):
    """Test get_user_download_decision with various valid inputs."""
    with patch("builtins.input", return_value=user_input):
        assert cli.get_user_download_decision(10) is expected


def test_get_user_download_decision_invalid_then_valid():
    """Test get_user_download_decision with one invalid then a valid input."""
    with patch("builtins.input", side_effect=["invalid", "y"]):
        assert cli.get_user_download_decision(10) is True


def test_get_user_download_decision_max_tries_exceeded():
    """Test get_user_download_decision raises ValueError after max_tries."""
    with patch("builtins.input", return_value="invalid"):
        with pytest.raises(
            ValueError, match="Received 3 incorrect inputs, terminating."
        ):
            cli.get_user_download_decision(10, max_tries=3)
