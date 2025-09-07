import logging
import tarfile
import urllib.request as request
from pathlib import Path
from time import perf_counter

from bundestag.data.utils import ensure_path_exists

logger = logging.getLogger(__name__)


def run(path: Path, dry: bool = False, assume_yes: bool = False):
    start_time = perf_counter()
    logger.info(f"Loading and extracting dataset from huggingface to {path.absolute()}")

    if not dry:
        if not path.exists():
            ensure_path_exists(path, assume_yes=assume_yes)

        # raw data

        logger.info("Downloading raw data from huggingface")
        request.urlretrieve(
            "https://huggingface.co/datasets/Bingpot/bundestag/resolve/main/raw.tar.gz",
            path / "raw.tar.gz",
        )
        logger.info("Done loading raw data from huggingface")

        raw_tar = path / "raw.tar.gz"
        logger.info(f"Extracting raw data at {raw_tar.absolute()}")
        tar = tarfile.open(raw_tar)
        tar.extractall(path=path)
        tar.close()
        logger.info("Done extracting raw data")

        # preprocessed data

        logger.info("Downloading preprocessed data from huggingface")
        request.urlretrieve(
            "https://huggingface.co/datasets/Bingpot/bundestag/resolve/main/preprocessed.tar.gz",
            path / "preprocessed.tar.gz",
        )
        logger.info("Done loading preprocessed data from huggingface")

        preprocessed_tar = path / "preprocessed.tar.gz"
        logger.info(f"Extracting preprocessed data at {preprocessed_tar.absolute()}")
        tar = tarfile.open(path / "preprocessed.tar.gz")
        tar.extractall(path=path)
        tar.close()
        logger.info("Done extracting preprocessed data")

    dt = str(perf_counter() - start_time)
    logger.info(
        f"Done loading and extracting dataset from huggingface to {path.absolute()} after {dt}"
    )
