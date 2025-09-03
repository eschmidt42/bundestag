import logging
import re

import polars as pl

logger = logging.getLogger(__name__)

PARTY_PATTERN = re.compile(r"(.+)\sseit")


def extract_party_from_string(s: str) -> str:
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
    if len(elements) > 0:
        return elements.map_elements(extract_party_from_string)
    else:
        return pl.Series([missing])
