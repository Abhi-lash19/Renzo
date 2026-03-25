from utils.logger import get_logger
from storage.db import init_db
from config.settings import settings

logger = get_logger("main")


def main():
    logger.info("🚀 Starting Job Intelligence Engine")

    # Initialize DB
    init_db()

    logger.info("Configuration Loaded:")
    logger.info(f"Adzuna Enabled: {bool(settings.ADZUNA_APP_ID)}")

    logger.info("✅ System initialized successfully")


if __name__ == "__main__":
    main()