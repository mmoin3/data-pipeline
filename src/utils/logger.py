import logging
from pathlib import Path

LOG_PATH = Path(__file__).parent.parent.parent / "logs" / "pipeline.log"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # file handler - writes to /logs/pipeline.log
        fh = logging.FileHandler(LOG_PATH)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        ))

        # console handler - prints to terminal
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        ))

        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
