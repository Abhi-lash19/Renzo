import requests
import time
from typing import Optional
from utils.logger import get_logger
from config.settings import settings

logger = get_logger(__name__)


def get_with_retry(url: str, params: Optional[dict] = None, retries: Optional[int] = None, timeout: Optional[int] = None) -> Optional[requests.Response]:
    """
    Perform HTTP GET request with retry logic and exponential backoff.

    Args:
        url: The URL to request
        params: Query parameters
        retries: Number of retry attempts (default from settings)
        timeout: Request timeout in seconds (default from settings)

    Returns:
        Response object if successful, None if all retries failed
    """
    if retries is None:
        retries = settings.RETRY_ATTEMPTS
    if timeout is None:
        timeout = settings.REQUEST_TIMEOUT

    backoff = 2

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                logger.debug(f"[{url}] Success on attempt {attempt + 1}")
                return response

            logger.warning(f"[{url}] Status: {response.status_code} on attempt {attempt + 1}")

        except requests.RequestException as e:
            logger.warning(f"[{url}] Attempt {attempt + 1} failed: {e}")

        if attempt < retries - 1:
            sleep_time = backoff ** attempt
            logger.debug(f"[{url}] Retrying in {sleep_time}s")
            time.sleep(sleep_time)

    logger.error(f"[{url}] Failed after {retries} retries")
    return None