import logging
import os
import sys

def get_logger(name: str = "energia-prep-2") -> logging.Logger:
    """
    Konfiguracja loggera:
    - format:  YYYY-mm-dd HH:MM:SS | LEVEL | energia-prep-2 | message
    - poziom z env LOG_LEVEL (domyślnie INFO)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)  # → STDOUT (Dozzle to lubi)
    handler.setFormatter(logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | energia-prep-2 | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

log = get_logger()
