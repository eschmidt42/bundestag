import typer

OPTION_DRY = typer.Option(False, help="Dry or not")
OPTION_DATA_PATH = typer.Option("data", help="Root dir for data storage")
ARGUMENT_LEGISLATURE_ID = typer.Argument(
    111,
    help="Bundestag legislature id value, see https://www.abgeordnetenwatch.de/bundestag -> Button 'Open Data'",
)
OPTION_Y = typer.Option(
    default=False,
    help="Assume yes to all prompts and run non-interactively.",
)
