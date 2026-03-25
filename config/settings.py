import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
    ADZUNA_API_KEY = os.getenv("ADZUNA_API_KEY")

    EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    # System configs
    JOB_FETCH_LIMIT = 500
    MAX_JOB_AGE_HOURS = 6

settings = Settings()