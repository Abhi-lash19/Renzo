from datetime import datetime
from typing import List, Dict, Any
from fetchers.base import BaseJobSource
from pipeline.models import Job
from config.settings import settings
from utils.http_client import get_with_retry
from utils.logger import get_logger

logger = get_logger(__name__)


class RemotiveFetcher(BaseJobSource):
    """Fetcher for Remotive job API."""

    URL = "https://remotive.com/api/remote-jobs"

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch jobs from Remotive API."""
        params = {
            "search": settings.SEARCH_KEYWORDS
        }

        response = get_with_retry(self.URL, params=params)

        if not response:
            return []

        data = response.json()
        jobs = data.get("jobs", [])

        logger.debug(f"Remotive: Fetched {len(jobs)} raw jobs")
        return jobs

    def normalize(self, raw: Dict[str, Any]) -> Job:
        """Normalize Remotive job data to Job object."""
        # Extract real posted_at from API response
        publication_date_str = raw.get("publication_date")
        if publication_date_str:
            try:
                # Assuming ISO format, e.g., "2023-10-01T12:00:00"
                posted_at = datetime.fromisoformat(publication_date_str)
            except ValueError:
                logger.warning(f"Invalid date format for Remotive job {raw.get('id')}: {publication_date_str}")
                posted_at = datetime.utcnow()
        else:
            posted_at = datetime.utcnow()

        return Job(
            job_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            company=raw.get("company_name", ""),
            location="Remote",
            description=raw.get("description", ""),
            url=raw.get("url", ""),
            source="remotive",
            posted_at=posted_at,
            fetched_at=datetime.utcnow()
        )