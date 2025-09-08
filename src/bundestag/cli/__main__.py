"""
# CLI `__main__`

This module is the main entry point for the `bundestag` command-line interface (CLI).
It uses [Typer](https://typer.tiangolo.com/) to create a simple and powerful CLI.

The main purpose of this CLI is to provide tools for:

1.  **Downloading**: Fetching raw data related to parliamentary proceedings.
2.  **Transforming**: Processing the raw data into a clean, usable format.

## Structure

The CLI is composed of sub-commands, each dedicated to a specific task.
The main sub-commands are:

-   `download`: Contains commands to download data from different sources like `abgeordnetenwatch.de` and `bundestag.de`.
-   `transform`: Contains commands to transform the downloaded raw data into a structured format.

## Usage

You can get help on the available commands by running:

```bash
bundestag --help
```

To get help for a specific subcommand, for example `download`, you can run:

```bash
bundestag download --help
```
"""

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
    """
    Set up logging for the CLI.
    This is a callback that is invoked before any command is executed.
    It configures the logging with a default level of INFO.
    """
    # configure logging for the CLI run
    setup_logging(logging.INFO)


if __name__ == "__main__":
    app()
