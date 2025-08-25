import logging

import pytest
from _pytest.logging import LogCaptureHandler
from inline_snapshot import snapshot
from rich.logging import RichHandler

from bundestag.fine_logging import setup_logging

logger = logging.getLogger(__name__)


def test_setup_logging_removes_existing_handlers():
    """Test that setup_logging removes any existing handlers on the root logger."""
    # Given: an existing handler on the root logger
    dummy_handler = logging.StreamHandler()
    logging.root.addHandler(dummy_handler)
    assert len(logging.root.handlers) > 0

    # When: setup_logging is called
    setup_logging()

    # Then: the existing handlers are removed and replaced by a RichHandler
    non_pytest_handlers = [
        h for h in logging.root.handlers if not isinstance(h, LogCaptureHandler)
    ]
    assert len(non_pytest_handlers) == 1
    assert isinstance(non_pytest_handlers[0], RichHandler)


def test_setup_logging_defaults():
    """Test that setup_logging configures the root logger correctly with default settings."""
    # When: setup_logging is called with default arguments
    setup_logging()

    # Then: the root logger's level is set to INFO
    assert logging.root.level == logging.INFO


def test_setup_logging_with_custom_level():
    """Test that setup_logging configures the root logger with a custom level."""
    # When: setup_logging is called with a custom level
    setup_logging(root_level=logging.DEBUG)

    # Then: the root logger's level is set to the custom level
    assert logging.root.level == logging.DEBUG


def test_setup_logging_is_idempotent():
    """Test that calling setup_logging multiple times doesn't add more handlers."""
    # When: setup_logging is called multiple times
    setup_logging()
    setup_logging()

    # Then: there is still only one handler
    non_pytest_handlers = [
        h for h in logging.root.handlers if not isinstance(h, LogCaptureHandler)
    ]
    assert len(non_pytest_handlers) == 1


def log_messages():
    """Log a dummy message."""
    logger.info("This is a dummy info log message.")
    logger.debug("This is a dummy debug log message.")


def test_log_a_message_with_setup_logging(caplog: pytest.LogCaptureFixture):
    """Test that log_a_message logs correctly after setup_logging."""

    setup_logging(
        root_level=logging.INFO,
        rich_level=logging.INFO,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    log_messages()

    assert caplog.text == snapshot(
        "INFO     tests.test_fine_logging:test_fine_logging.py:64 This is a dummy info log message.\n"
    )
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "INFO"

    setup_logging(
        root_level=logging.DEBUG,
        rich_level=logging.DEBUG,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    log_messages()

    assert caplog.text == snapshot("""\
INFO     tests.test_fine_logging:test_fine_logging.py:64 This is a dummy info log message.
INFO     tests.test_fine_logging:test_fine_logging.py:64 This is a dummy info log message.
DEBUG    tests.test_fine_logging:test_fine_logging.py:65 This is a dummy debug log message.
""")

    assert len(caplog.records) == 3
    assert caplog.records[0].levelname == "INFO"
    assert caplog.records[1].levelname == "INFO"
    assert caplog.records[2].levelname == "DEBUG"
