import logging
import os

LOG_DIR = "logs"
LOG_FILE = "app.log"
DEFAULT_LEVEL = "INFO"

os.makedirs(LOG_DIR, exist_ok=True)


def _get_console_level() -> int:
    level_name = os.getenv("LOG_LEVEL", DEFAULT_LEVEL).upper()
    if level_name not in logging._nameToLevel:
        level_name = DEFAULT_LEVEL
    return logging._nameToLevel[level_name]


def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        file_handler = logging.FileHandler(f"{LOG_DIR}/{LOG_FILE}")
        console_handler = logging.StreamHandler()

        # File → DEBUG, Console → configurable via LOG_LEVEL
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(_get_console_level())

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger