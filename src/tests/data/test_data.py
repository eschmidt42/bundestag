from pathlib import Path
from unittest.mock import MagicMock, patch

import bundestag.data as bdata


def test_get_multiple_sheets():
    "Testing the function that gets multiple sheets, mocking the downloading step."
    data_path = Path("src/tests/data_for_testing")
    html_path = data_path
    sheet_path = data_path
    dry = False
    validate = True

    with patch(
        "bundestag.data.download.bundestag_sheets.download_multiple_sheets",
        MagicMock(),
    ) as mock_download:
        # line to test
        df = bdata.get_multiple_sheets(
            html_path, sheet_path, dry=dry, validate=validate
        )

        # check that the download function was called
        mock_download.assert_called_once()
