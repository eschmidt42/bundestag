import logging
import logging.config

from rich.logging import RichHandler


def get_config(level: logging._Level) -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {"rich": RichHandler(rich_tracebacks=True)},
        "loggers": {"root": {"level": level}},
    }


def setup_logging(level=logging.INFO):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    config = get_config(level)
    logging.config.dictConfig(config)
