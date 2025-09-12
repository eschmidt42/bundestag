import logging
from unittest.mock import patch

from typer.testing import CliRunner

from bundestag.cli.__main__ import app

runner = CliRunner()


def test_app_help():
    """
    Test the main help message of the CLI.
    It should show the main subcommands.
    """
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "download" in result.stdout
    assert "transform" in result.stdout


@patch("bundestag.cli.__main__.setup_logging")
def test_logging_is_setup(mock_setup_logging):
    """
    Test that the logging setup function is called.
    """
    result = runner.invoke(app, ["download", "--help"])
    assert result.exit_code == 0
    mock_setup_logging.assert_called_once_with(logging.INFO)
