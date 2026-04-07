from datetime import datetime
from typing import List, Dict, Any
from fetchers.base import BaseJobSource
from pipeline.models import Job
from config.settings import settings
from utils.http_client import get_with_retry
from utils.logger import get_logger

logger = get_logger(__name__)


class AdzunaFetcher(BaseJobSource):
    """Fetcher for Adzuna job API."""

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/in/search/1"

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch jobs from Adzuna API with pagination."""
        all_results = []
        results_per_page = 50

        for page in range(settings.PAGINATION_PAGES):
            start = page * results_per_page
            params = {
                "app_id": settings.ADZUNA_APP_ID,
                "app_key": settings.ADZUNA_API_KEY,
                "results_per_page": results_per_page,
                "start": start,
                "what": settings.SEARCH_KEYWORDS
            }

            response = get_with_retry(self.BASE_URL, params=params)

            if not response:
                logger.warning(f"Adzuna: Failed to fetch page {page + 1}")
                break

            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)

            # If fewer results than requested, no more pages
            if len(results) < results_per_page:
                break

        logger.debug(f"Adzuna: Fetched {len(all_results)} raw jobs")
        return all_results

    def normalize(self, raw: Dict[str, Any]) -> Job:
        """Normalize Adzuna job data to Job object."""
        # Extract real posted_at from API response
        created_str = raw.get("created")
        if created_str:
            try:
                # Adzuna dates are in ISO format, e.g., "2023-10-01T12:00:00Z"
                posted_at = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
            except ValueError:
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