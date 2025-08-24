import json
import logging
from pathlib import Path

import pandas as pd

import bundestag.data.utils as data_utils
import bundestag.schemas as schemas

logger = logging.getLogger(__name__)


def load_mandate_json(legislature_id: int, path: Path, dry: bool = False) -> dict:
    mandates_fname = data_utils.get_mandates_filename(legislature_id)
    file = data_utils.get_location(
        mandates_fname,
        path=path,
        dry=dry,
        mkdir=False,
    )

    logger.debug(f"Reading mandates info from {file}")
    with open(file, "r", encoding="utf8") as f:
        info = json.load(f)
    return info


def parse_mandate_data(mandate: schemas.Mandate, missing: str = "unknown") -> dict:
    d = {
        "legislature_id": mandate.parliament_period.id,
        "legislature_period": mandate.parliament_period.label,
        "mandate_id": mandate.id,
        "mandate": mandate.label,
        "politician_id": mandate.politician.id,
        "politician": mandate.politician.label,
        "politician_url": mandate.politician.abgeordnetenwatch_url,
        "start_date": mandate.start_date,
        "end_date": "" if mandate.end_date is None else mandate.end_date,
        "constituency_id": mandate.electoral_data.constituency.id
        if mandate.electoral_data.constituency is not None
        else None,
        "constituency_name": mandate.electoral_data.constituency.label
        if mandate.electoral_data.constituency is not None
        else None,
    }

    if len(mandate.fraction_membership) > 0:
        d_fraction = {
            "fraction_names": [_m.label for _m in mandate.fraction_membership],
            "fraction_ids": [_m.id for _m in mandate.fraction_membership],
            "fraction_starts": [_m.valid_from for _m in mandate.fraction_membership],
            "fraction_ends": [
                "" if _m.valid_until is None else _m.valid_until
                for _m in mandate.fraction_membership
            ],
        }
    else:
        d_fraction = {
            "fraction_names": [missing],
            "fraction_ids": [missing],
            "fraction_starts": [missing],
            "fraction_ends": [missing],
        }
    d.update(d_fraction)
    return d


def get_mandates_data(legislature_id: int, path: Path) -> pd.DataFrame:
    "Parses info from mandate json file(s) for `legislature_id`"
    info = load_mandate_json(legislature_id, path=path)
    mandates = schemas.MandatesResponse(**info)
    df = pd.DataFrame([parse_mandate_data(m) for m in mandates.data])
    return df
