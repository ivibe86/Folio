"""
log_config.py
Centralized logging configuration for the Folio backend.
"""

import logging
import os
import sys

_initialized = False

LOG_FORMAT = "%(asctime)s %(levelname)s [%(module)s]: %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging() -> None:
    """
    Configure the root logger with a stdout stream handler.
    Safe to call multiple times — only applies configuration once.
    """
    global _initialized
    if _initialized:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if someone calls setup_logging() again
    if not root.handlers:
        root.addHandler(handler)

    _initialized = True


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger, ensuring logging is configured first.
    Usage at module level:
        from log_config import get_logger
        logger = get_logger(__name__)
    """
    setup_logging()
    return logging.getLogger(name)