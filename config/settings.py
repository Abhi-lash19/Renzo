import os
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv()


def _mask(value: str) -> str:
    if not value:
        return "None"
    return value[:3] + "****" + value[-2:]


class Settings:
    # API Keys
    ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
    ADZUNA_API_KEY = os.getenv("ADZUNA_API_KEY")

    # Email configs
    EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    # Fetcher configs
    SEARCH_KEYWORDS = os.getenv("SEARCH_KEYWORDS", "python developer")
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
    PAGINATION_PAGES = int(os.getenv("PAGINATION_PAGES", "3"))
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
    RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))

    # System configs
    JOB_FETCH_LIMIT = int(os.getenv("JOB_FETCH_LIMIT", "500"))
    MAX_JOB_AGE_HOURS = int(os.getenv("MAX_JOB_AGE_HOURS", "6"))


settings = Settings()

# Debug once at startup
logger.debug(
    f"Settings loaded → "
    f"ADZUNA_APP_ID={_mask(settings.ADZUNA_APP_ID)}, "
    f"SEARCH_KEYWORDS='{settings.SEARCH_KEYWORDS}'"
)