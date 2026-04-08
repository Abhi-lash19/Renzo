from datetime import datetime
from typing import List, Dict, Any
from fetchers.base import BaseJobSource
from pipeline.models import Job
from config.settings import settings
from utils.http_client import get_with_retry
from utils.logger import get_logger

logger = get_logger(__name__)


def _mask(value: str) -> str:
    """Mask sensitive values for logging."""
    if not value:
        return "None"
    return value[:3] + "****" + value[-2:]


class AdzunaFetcher(BaseJobSource):
    """Fetcher for Adzuna job API."""


    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch jobs from Adzuna API with pagination."""

        #  Log credentials safely
        logger.debug(
            f"Adzuna creds → app_id={_mask(settings.ADZUNA_APP_ID)}, "
            f"key={_mask(settings.ADZUNA_API_KEY)}"
        )

        # Validate credentials
        if not settings.ADZUNA_APP_ID or not settings.ADZUNA_API_KEY:
            logger.warning("Adzuna: API credentials not configured in .env file - skipping")
            return []

        if settings.ADZUNA_APP_ID == "your_app_id" or settings.ADZUNA_API_KEY == "your_api_key":
            logger.warning("Adzuna: Using placeholder API credentials. Please update .env - skipping")
            return []

        # Ensure search keyword exists
        search_query = settings.SEARCH_KEYWORDS.strip() if settings.SEARCH_KEYWORDS else "developer"

        if not search_query:
            logger.warning("Adzuna: SEARCH_KEYWORDS is empty, defaulting to 'developer'")
            search_query = "developer"

        logger.info(f"🔍 Adzuna: Starting fetch with query='{search_query}'")

        all_results = []
        results_per_page = 50

        for page in range(settings.PAGINATION_PAGES):
            params = {
                "app_id": settings.ADZUNA_APP_ID,
                "app_key": settings.ADZUNA_API_KEY,
                "results_per_page": results_per_page,
                "what": search_query
            }

            # Safe params logging
            safe_params = {k: ("****" if "key" in k or "app_id" in k else v) for k, v in params.items()}
            logger.debug(f"Adzuna request params (page {page+1}): {safe_params}")

            # Correct endpoint: page starts from 1
            url = f"https://api.adzuna.com/v1/api/jobs/in/search/{page + 1}"
            logger.debug(f"Adzuna URL: {url}")
            
            response = get_with_retry(url, params=params)

            if not response:
                logger.warning(f"Adzuna: Failed to fetch page {page + 1}")
                if page == 0:
                    # First page failed, likely credential or endpoint issue
                    logger.warning("Adzuna: First page failed - stopping fetch")
                break

            try:
                data = response.json()

                # Debug response structure
                if "results" not in data:
                    logger.warning(f"Adzuna: Unexpected response format - no 'results' key. Keys: {list(data.keys())}")
                    logger.debug(f"Adzuna response: {data}")
                    break

                results = data.get("results", [])
                logger.info(f"✅ Adzuna page {page+1}: fetched {len(results)} jobs")

                all_results.extend(results)

                if len(results) < results_per_page:
                    logger.debug(f"Adzuna: Got {len(results)} < {results_per_page}, stopping pagination")
                    break

            except Exception as e:
                logger.error(f"Adzuna: Failed to parse response on page {page + 1}: {e}")
                logger.debug(f"Raw response: {response.text[:500]}")
                break

        logger.info(f"🎯 Adzuna: Total fetched {len(all_results)} jobs")
        return all_results

    def normalize(self, raw: Dict[str, Any]) -> Job:
        """Normalize Adzuna job data to Job object."""

        created_str = raw.get("created")

        if created_str:
            try:
                posted_at = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
                if posted_at.tzinfo is not None:
                    posted_at = posted_at.replace(tzinfo=None)
            except (ValueError, TypeError):
                logger.warning(f"Invalid date format for job {raw.get('id')}: {created_str}")
                posted_at = datetime.utcnow()
        else:
            posted_at = datetime.utcnow()

        return Job(
            job_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            company=raw.get("company", {}).get("display_name", ""),
            location=raw.get("location", {}).get("display_name", ""),
            description=raw.get("description", ""),
            url=raw.get("redirect_url", ""),
            source="adzuna",
            posted_at=posted_at,
            fetched_at=datetime.utcnow()
        )