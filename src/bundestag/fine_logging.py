import logging

from rich.logging import RichHandler


def setup_logging(
    root_level=logging.INFO,
    rich_level=logging.INFO,
    fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
):
    """Configures logging for the application using rich for pretty output.

    This function sets up a root logger with a `RichHandler`. It's designed
    to be idempotent, meaning it can be called multiple times without creating
    duplicate handlers. It also preserves pytest's `LogCaptureHandler` to ensure
    log capturing in tests is not disturbed.

    Args:
        root_level (int, optional): The logging level for the root logger.
                                    Defaults to `logging.INFO`.
        rich_level (int, optional): The logging level for the `RichHandler`.
                                    Defaults to `logging.INFO`.
        fmt (str, optional): The format string for the log messages.
                             Defaults to "%(asctime)s - %(name)s - %(levelname)s - %(message)s".
    """
    # If logging is already configured with a single RichHandler at the
    # requested level, do nothing (idempotent).
    is_same_level = logging.root.level == root_level
    is_same_root_count = len(logging.root.handlers) == 1
    is_same_rich_handler = is_same_root_count and isinstance(
        logging.root.handlers[0], RichHandler
    )
    if is_same_rich_handler and is_same_level:
        return

    # Detect pytest's capture handler (LogCaptureHandler) by name and
    # preserve it so caplog can continue capturing logs in tests.
    capture_handlers = [
        h for h in logging.root.handlers if h.__class__.__name__ == "LogCaptureHandler"
    ]

    # Create or reuse a RichHandler
    rich_handler = None
    for h in logging.root.handlers:
        if isinstance(h, RichHandler):
            rich_handler = h
            break
    if rich_handler is None:
        rich_handler = RichHandler()

    rich_handler.setLevel(rich_level)

    # Apply formatter: accept either a format string or a logging.Formatter
    if fmt is not None:
        if isinstance(fmt, logging.Formatter):
            formatter = fmt
        else:
            formatter = logging.Formatter(fmt)
        rich_handler.setFormatter(formatter)

    # Build new handlers list: always include capture handlers (if any),
    # then the RichHandler. If there were no capture handlers, this
    # replaces existing handlers with a single RichHandler as expected.
    new_handlers = capture_handlers[:]  # copy
    # Avoid adding the same RichHandler twice
    if rich_handler not in new_handlers:
        new_handlers.append(rich_handler)

    logging.root.handlers = new_handlers
    logging.root.setLevel(root_level)
