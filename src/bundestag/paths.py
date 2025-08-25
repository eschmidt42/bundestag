import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Paths:
    root_path: Path
    raw: str = "raw"
    preprocessed: str = "preprocessed"
    abgeordnetenwatch: str = "abgeordnetenwatch"
    bundestag: str = "bundestag"

    def __post_init__(self):
        self.raw_base = self.root_path / self.raw
        self.raw_abgeordnetenwatch = self.raw_base / self.abgeordnetenwatch
        self.raw_bundestag = self.raw_base / self.bundestag
        self.raw_bundestag_html = self.raw_bundestag / "htm_files"
        self.raw_bundestag_sheets = self.raw_bundestag / "sheets"

        self.preprocessed_base = self.root_path / self.preprocessed
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


def get_paths(root_path: str | Path) -> Paths:
    path = Path(root_path)
    return Paths(path)


PATHS = get_paths("data")
