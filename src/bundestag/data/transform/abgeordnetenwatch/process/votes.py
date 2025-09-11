import json
import logging
from pathlib import Path

import polars as pl
from tqdm import tqdm

import bundestag.schemas as schemas
from bundestag.data.download.abgeordnetenwatch.store import check_stored_vote_ids
from bundestag.data.utils import get_location, get_votes_filename

logger = logging.getLogger(__name__)


def load_vote_json(legislature_id: int, poll_id: int, path: Path) -> dict:
    """Loads vote data from a JSON file for a given legislature and poll.

    Args:
        legislature_id (int): The ID of the legislature.
        poll_id (int): The ID of the poll.
        path (Path): The path to the directory containing the vote data files.

    Returns:
        dict: A dictionary containing the vote data from the JSON file.
    """
    votes_fname = get_votes_filename(legislature_id, poll_id)
    file = get_location(
        votes_fname,
        path=path,
        dry=False,
        mkdir=False,
    )

    logger.debug(f"Reading vote info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_vote_data(vote: schemas.Vote) -> dict:
    """Parses a single vote object into a dictionary.

    Args:
        vote (schemas.Vote): The vote object to parse.

    Returns:
        dict: A dictionary containing the parsed vote data.
    """
    d = {
        "mandate_id": vote.mandate.id,
        "mandate": vote.mandate.label,
        "poll_id": vote.poll.id,
        "vote": vote.vote,
        "reason_no_show": vote.reason_no_show,
        "reason_no_show_other": vote.reason_no_show_other,
    }

    return d


SCHEMA_GET_VOTES_DATA = pl.Schema(
    {
        "mandate_id": pl.Int64(),
        "mandate": pl.String(),
        "poll_id": pl.Int64(),
        "vote": pl.String(),
        "reason_no_show": pl.String(),
        "reason_no_show_other": pl.String(),
    }
)


def get_votes_data(
    legislature_id: int,
    poll_id: int,
    path: Path,
    validate: bool = False,
) -> pl.DataFrame:
    """Parses vote information from a JSON file for a specific poll and returns it as a Polars DataFrame.

    Args:
        legislature_id (int): The ID of the legislature.
        poll_id (int): The ID of the poll.
        path (Path): The path to the directory containing the vote data files.
        validate (bool, optional): A flag for validation (currently unused). Defaults to False.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the parsed vote data for the specified poll.
    """

    data = load_vote_json(legislature_id, poll_id, path=path)
    votes = schemas.VoteResponse(**data)
    n_none = 0
    df = []
    for vote in votes.data.related_data.votes:
        if vote.id is None:
            n_none += 1
            continue
        vote = parse_vote_data(vote)
        df.append(vote)

    if n_none > 0:
        logger.warning(f"Removed {n_none} votes because of their id being None")
    df = pl.DataFrame(df, schema=SCHEMA_GET_VOTES_DATA)

    return df


def compile_votes_data(
    legislature_id: int, path: Path, validate: bool = False
) -> pl.DataFrame:
    """Compiles the individual politicians' votes for a specific legislature period into a single DataFrame.

    This function iterates through all the stored vote files for a given legislature,
    loads the data for each poll, and concatenates them into one large DataFrame.
    It also handles and logs duplicate `mandate_id` entries within a single poll's data.

    Args:
        legislature_id (int): The ID of the legislature for which to compile the votes.
        path (Path): The path to the directory containing the vote data files.
        validate (bool, optional): A flag for validation (currently unused). Defaults to False.

    Returns:
        pl.DataFrame: A Polars DataFrame containing all the vote data for the specified legislature.
    """

    known_id_combos = check_stored_vote_ids(legislature_id=legislature_id, path=path)

    # TODO: figure out why some mandate_id entries are duplicate in vote_json files

    df_all_votes = []
    for poll_id in tqdm(
        known_id_combos[legislature_id],
        total=len(known_id_combos[legislature_id]),
        desc="poll_id",
    ):
        df = get_votes_data(legislature_id, poll_id, path=path, validate=False)

        id_duplicates = df.filter(pl.col("mandate_id").is_duplicated())[
            "mandate_id"
        ].unique()

        if len(id_duplicates) > 0:
            logger.warning(
                f"Dropping duplicates for mandate_ids ({len(id_duplicates):_}):\n{df.filter(pl.col('mandate_id').is_in(pl.lit(id_duplicates)))}"
            )
            df = df.unique("mandate_id", keep="first", maintain_order=True)

        df_all_votes.append(df)

    df_all_votes = pl.concat(df_all_votes)

    return df_all_votes
