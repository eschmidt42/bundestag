import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Paths:
    """A dataclass to manage all relevant paths for the project's data directory.

    This class centralizes the definition of the directory structure for raw and
    preprocessed data, making it easier to manage and access files throughout
    the project. Upon initialization, it constructs `pathlib.Path` objects for
    various subdirectories.

    Attributes:
        root_path (Path): The root directory for the data (e.g., 'data/').
        raw (str): The name of the raw data directory.
        preprocessed (str): The name of the preprocessed data directory.
        abgeordnetenwatch (str): The subdirectory name for Abgeordnetenwatch data.
        bundestag (str): The subdirectory name for Bundestag data.
        raw_base (Path): The full path to the raw data directory.
        raw_abgeordnetenwatch (Path): Path to raw Abgeordnetenwatch data.
        raw_bundestag (Path): Path to raw Bundestag data.
        raw_bundestag_html (Path): Path to raw Bundestag HTML files.
        raw_bundestag_sheets (Path): Path to raw Bundestag Excel sheets.
        preprocessed_base (Path): The full path to the preprocessed data directory.
        preprocessed_abgeordnetenwatch (Path): Path to preprocessed Abgeordnetenwatch data.
        preprocessed_bundestag (Path): Path to preprocessed Bundestag data.
    """

    root_path: Path
    raw: str = "raw"
    preprocessed: str = "preprocessed"
    abgeordnetenwatch: str = "abgeordnetenwatch"
    bundestag: str = "bundestag"

    def __post_init__(self):
        """Constructs the full paths to subdirectories after the dataclass is initialized."""
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
        """Creates the directory structure for raw data.

        This method iterates through the defined raw data paths and creates the
        corresponding directories on the filesystem.

        Args:
            dry (bool, optional): If True, the method will only log the paths
                                  that would be created without actually creating
                                  them. Defaults to False.
        """
        logger.debug("Creating paths for raw data")
        for path in [self.raw_bundestag_html, self.raw_bundestag_sheets]:
            logger.debug(f"Creating {path}")
            if dry:
                continue
            path.mkdir(exist_ok=True, parents=True)

    def make_preprocessed_paths(self, dry: bool = False):
        """Creates the directory structure for preprocessed data.

        This method iterates through the defined preprocessed data paths and
        creates the corresponding directories on the filesystem.

        Args:
            dry (bool, optional): If True, the method will only log the paths
                                  that would be created without actually creating
                                  them. Defaults to False.
        """
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
        """Creates all necessary directory structures for both raw and preprocessed data."""
        self.make_raw_paths()
        self.make_preprocessed_paths()


def get_paths(root_path: str | Path) -> Paths:
    """A factory function to create a Paths object.

    Args:
        root_path (str | Path): The root path for the data directory.

    Returns:
        Paths: An initialized Paths object.
    """
    path = Path(root_path)
    return Paths(path)


PATHS = get_paths("data")
