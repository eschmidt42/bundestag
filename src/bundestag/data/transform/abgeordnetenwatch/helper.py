import re

import pandas as pd

import bundestag.logging as logging

logger = logging.logger

PARTY_PATTERN = re.compile(r"(.+)\sseit")


def extract_party_from_string(s: str) -> str:
    if not isinstance(s, str):
        raise ValueError(f"Expected {s=} to be of type string.")
    elif "seit" in s:
        return PARTY_PATTERN.search(s).groups()[0]
    else:
        return s


def get_parties_from_col(
    row: pd.Series, col="fraction_names", missing: str = "unknown"
):
    strings = row[col]
    if strings is None or not isinstance(strings, list):
        return [missing]
    else:
        return [extract_party_from_string(s) for s in strings]


def get_politician_names(df: pd.DataFrame, col="mandate"):
    names = df[col].str.split(" ").str[:-4].str.join(" ")
    logger.debug(f"Parsing `{col}` to names. Found {names.nunique()} names")
    return names
