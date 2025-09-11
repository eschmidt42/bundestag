import logging
from unittest.mock import patch

from typer.testing import CliRunner

from bundestag.get_xlsx_uris import app

runner = CliRunner()


@patch("bundestag.get_xlsx_uris.create_xlsx_uris_dict")
@patch("bundestag.get_xlsx_uris.store_xlsx_uris")
def test_run_command(mock_store_xlsx_uris, mock_create_xlsx_uris_dict, tmp_path):
    """
    Test the run command of the get_xlsx_uris CLI.
    It should call the create and store functions with the correct arguments.
    """
    # Arrange
    json_path = tmp_path / "xlsx_uris.json"
    mock_create_xlsx_uris_dict.return_value = {"uri1": "url1", "uri2": "url2"}

    # Act
    result = runner.invoke(
        app, ["run", "--max-pages", "5", "--json-path", str(json_path)]
    )

    # Assert
    assert result.exit_code == 0
    mock_create_xlsx_uris_dict.assert_called_once_with(max_pages=5)
    mock_store_xlsx_uris.assert_called_once_with(
        json_path, {"uri1": "url1", "uri2": "url2"}
    )
    assert "Starting xlsx uris collection" in result.stdout
    assert "Finished xlsx uris collection" in result.stdout


@patch("bundestag.get_xlsx_uris.setup_logging")
def test_logging_is_setup(mock_setup_logging):
    """
    Test that the logging setup function is called.
    """
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    mock_setup_logging.assert_called_once_with(logging.INFO)
