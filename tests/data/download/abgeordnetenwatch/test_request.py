from unittest.mock import Mock, patch

import pytest

from bundestag.data.download.abgeordnetenwatch.request import (
    request_mandates_data,
    request_poll_data,
    request_vote_data,
)


class TestRequestPollData:
    def test_request_poll_data_dry_mode(self):
        result = request_poll_data(legislature_id=111, dry=True)
        assert result is None

    @patch("httpx.get")
    def test_request_poll_data_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test"}
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        result = request_poll_data(legislature_id=111, num_polls=500)

        assert result == {"data": "test"}
        mock_get.assert_called_once_with(
            "https://www.abgeordnetenwatch.de/api/v2/polls",
            params={"field_legislature": 111, "range_end": 500},
            timeout=42,
        )

    @patch("httpx.get")
    def test_request_poll_data_failure(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        with pytest.raises(AssertionError, match="Unexpected GET status: 404"):
            request_poll_data(legislature_id=111)


class TestRequestMandatesData:
    def test_request_mandates_data_dry_mode(self):
        result = request_mandates_data(legislature_id=111, dry=True)
        assert result is None

    @patch("httpx.get")
    def test_request_mandates_data_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"mandates": "data"}
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        result = request_mandates_data(legislature_id=111, num_mandates=300)

        assert result == {"mandates": "data"}
        mock_get.assert_called_once_with(
            "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates",
            params={"parliament_period": 111, "range_end": 300},
            timeout=42,
        )

    @patch("httpx.get")
    def test_request_mandates_data_failure(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        with pytest.raises(AssertionError, match="Unexpected GET status: 500"):
            request_mandates_data(legislature_id=111)


class TestRequestVoteData:
    def test_request_vote_data_dry_mode(self):
        result = request_vote_data(poll_id=123, dry=True)
        assert result is None

    @patch("httpx.get")
    def test_request_vote_data_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"votes": "data"}
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        result = request_vote_data(poll_id=123)

        assert result == {"votes": "data"}
        mock_get.assert_called_once_with(
            "https://www.abgeordnetenwatch.de/api/v2/polls/123",
            params={"related_data": "votes", "range_end": 999},
            timeout=42,
        )

    @patch("httpx.get")
    def test_request_vote_data_failure(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 403
        mock_response.url = "test_url"
        mock_get.return_value = mock_response

        with pytest.raises(AssertionError, match="Unexpected GET status: 403"):
            request_vote_data(poll_id=123)
