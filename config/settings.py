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

    # Database configs
    DB_TYPE = os.getenv("DB_TYPE", "sqlite")              # "sqlite" | "postgres"
    DB_PATH = os.getenv("DB_PATH", "data/jobs.db")        # SQLite file path
    PG_DSN = os.getenv("PG_DSN", "")                      # Postgres DSN
    LOG_FORMAT = os.getenv("LOG_FORMAT", "readable")       # "readable" | "json"

    # Supabase / Auth
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
    SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
    SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")

    # Auth gate — set to "true" to enforce JWT on all non-health endpoints
    AUTH_ENABLED = os.getenv("AUTH_ENABLED", "false").lower() == "true"

    # DB backend alias (mirrors DB_TYPE for clarity in Phase 3+)
    DB_BACKEND = os.getenv("DB_BACKEND", os.getenv("DB_TYPE", "sqlite")).lower()

    # Embeddings / Hybrid Retrieval
    EMBEDDINGS_ENABLED = os.getenv("EMBEDDINGS_ENABLED", "false").lower() == "true"
    EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "BAAI/bge-large-en-v1.5")
    EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "bge")  # "bge" | "mock"
    RETRIEVAL_KEYWORD_WEIGHT = float(os.getenv("RETRIEVAL_KEYWORD_WEIGHT", "0.60"))
    RETRIEVAL_VECTOR_WEIGHT = float(os.getenv("RETRIEVAL_VECTOR_WEIGHT", "0.40"))
    EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))


settings = Settings()

# Debug once at startup
logger.debug(
    f"Settings loaded → "
    f"ADZUNA_APP_ID={_mask(settings.ADZUNA_APP_ID)}, "
    f"SEARCH_KEYWORDS='{settings.SEARCH_KEYWORDS}'"
)