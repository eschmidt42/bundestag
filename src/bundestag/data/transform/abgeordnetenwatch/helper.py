import logging
import re

import polars as pl

logger = logging.getLogger(__name__)

PARTY_PATTERN = re.compile(r"(.+)\sseit")


def extract_party_from_string(s: str) -> str:
    """Extracts a party name from a string.

    The function is designed to handle strings that may contain additional information after the party name,
    specifically patterns like "Party Name seit YYYY-MM-DD". It uses a regex to find the party name.

    Args:
        s (str): The input string, potentially containing a party name.

    Raises:
        ValueError: If the input is not a string, or if the regex fails to find a match in a string containing "seit".

    Returns:
        str: The extracted party name.
    """
    if not isinstance(s, str):
        raise ValueError(f"Expected {s=} to be of type string.")
    elif "seit" in s:
        match = PARTY_PATTERN.search(s)
        if match is None:
            raise ValueError(f"failed to match {PARTY_PATTERN=}/")
        return match.groups()[0]
    else:
        return s


def get_parties_from_col(elements: pl.Series, missing: str = "unknown") -> pl.Series:
    """Extracts party names from a Polars Series of strings.

    This function applies the `extract_party_from_string` function to each element of a Polars Series.
    If the series is empty, it returns a series with a single 'unknown' value.

    Args:
        elements (pl.Series): A Polars Series containing strings with party information.
        missing (str, optional): The value to return if the input series is empty. Defaults to "unknown".

    Returns:
        pl.Series: A Polars Series with the extracted party names.
    """
    if len(elements) > 0:
        return elements.map_elements(extract_party_from_string)
    else:
        return pl.Series([missing])
