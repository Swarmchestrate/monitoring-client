import logging
import sys


def configure_stdout_logger(name: str) -> logging.Logger:
    """Return a logger configured to write plain messages to stdout."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = next(
        (
            existing_handler
            for existing_handler in logger.handlers
            if getattr(existing_handler, "_swchmon_stdout_handler", False)
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler(sys.stdout)
        handler._swchmon_stdout_handler = True
        logger.addHandler(handler)
    else:
        handler.setStream(sys.stdout)

    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    return logger