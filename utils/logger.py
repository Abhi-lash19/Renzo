import logging
import os
import sys

LOG_DIR = "logs"
LOG_FILE = "app.log"
DEFAULT_LEVEL = "INFO"

os.makedirs(LOG_DIR, exist_ok=True)

class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError:
            try:
                # Fallback: strip non-ascii characters to avoid crash
                record.msg = str(record.msg).encode('ascii', 'ignore').decode('ascii')
                super().emit(record)
            except Exception:
                pass
        except Exception:
            pass

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
        file_handler = logging.FileHandler(f"{LOG_DIR}/{LOG_FILE}", encoding="utf-8")
        
        # Force encoding layout
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except Exception:
            pass
            
        console_handler = SafeConsoleHandler(stream=sys.stdout)

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