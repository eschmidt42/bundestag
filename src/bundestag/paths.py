from dataclasses import dataclass
from pathlib import Path

import bundestag.logging as logging

logger = logging.logger


@dataclass
class Paths:
    base: Path
    raw: str = "raw"
    preprocessed: str = "preprocessed"
    abgeordnetenwatch: str = "abgeordnetenwatch"
    bundestag: str = "bundestag"

    def __post_init__(self):
        self.raw_base = self.base / self.raw
        self.raw_abgeordnetenwatch = self.raw_base / self.abgeordnetenwatch
        self.raw_bundestag = self.raw_base / self.bundestag
        self.raw_bundestag_html = self.raw_bundestag / "htm_files"
        self.raw_bundestag_sheets = self.raw_bundestag / "sheets"

        self.preprocessed_base = self.base / self.preprocessed
        self.preprocessed_abgeordnetenwatch = (
            self.preprocessed_base / self.abgeordnetenwatch
        )
        self.preprocessed_bundestag = self.preprocessed_base / self.bundestag

    def make_raw_paths(self, dry: bool = False):
        logger.debug("Creating paths for raw data")
        for path in [self.raw_bundestag_html, self.raw_bundestag_sheets]:
            logger.debug(f"Creating {path}")
            if dry:
                continue
            path.mkdir(exist_ok=True, parents=True)

    def make_preprocessed_paths(self, dry: bool = False):
        logger.debug("Creating paths for preprocessed data")
        for path in [
            self.preprocessed_abgeordnetenwatch,
            self.preprocessed_bundestag,
        ]:
            logger.debug(f"Creating {path}")
            if dry:
                continue
            path.mkdir(exist_ok=True, parents=True)

    def make_paths(self):
        self.make_raw_paths()
        self.make_preprocessed_paths()


def get_paths(base: str) -> Paths:
    base = Path(base)
    return Paths(base)


PATHS = get_paths("data")
