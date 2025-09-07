import logging

import typer

from bundestag.cli.download import app as download_app
from bundestag.cli.transform import app as transform_app
from bundestag.fine_logging import setup_logging

logger = logging.getLogger(__name__)

app = typer.Typer()
app.add_typer(download_app, name="download")
app.add_typer(transform_app, name="transform")


@app.callback(invoke_without_command=True)
def _setup_logging_callback():
    # configure logging for the CLI run
    setup_logging(logging.INFO)


if __name__ == "__main__":
    app()
