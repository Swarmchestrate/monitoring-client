import logging
import sys


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_stdout_logger(name: str) -> logging.Logger:
    """Return a logger configured like the listener example, but on stdout."""
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
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return logger