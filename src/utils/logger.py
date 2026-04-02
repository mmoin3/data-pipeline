import logging
from datetime import date, datetime
from pathlib import Path


def get_log_dir():
    """Lazy import to avoid circular dependencies."""
    from config import LOG_DIR
    return LOG_DIR


LOG_PATH = None  # Will be set lazily


class PeriodFormatter(logging.Formatter):
    """Use period instead of comma for milliseconds."""

    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
        return f"{s}.{int(record.msecs):04d}"


def get_logger(name: str) -> logging.Logger:
    global LOG_PATH

    # Set LOG_PATH on first call (lazy initialization)
    if LOG_PATH is None:
        LOG_PATH = get_log_dir() / \
            f"pipeline_{date.today().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # file handler
        logger.handlers.clear()  # Clear any existing handlers
        fh = logging.FileHandler(LOG_PATH)
        fh.setFormatter(PeriodFormatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        ))

        # console handler
        ch = logging.StreamHandler()
        ch.setFormatter(PeriodFormatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        ))

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
