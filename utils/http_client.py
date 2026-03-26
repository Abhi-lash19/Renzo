import requests
import time
from utils.logger import get_logger

logger = get_logger(__name__)


def get_with_retry(url, params=None, retries=3, timeout=10, backoff=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                return response

            logger.warning(f"[{url}] Status: {response.status_code}")

        except requests.RequestException as e:
            logger.warning(f"[{url}] Attempt {attempt+1} failed: {e}")

        time.sleep(backoff ** attempt)  # exponential backoff

    logger.error(f"[{url}] Failed after {retries} retries")
    return None