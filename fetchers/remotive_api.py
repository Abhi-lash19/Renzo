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
        """Fetch jobs from Remotive API with fallback."""
        
        # Try with configured search keywords first
        search_term = settings.SEARCH_KEYWORDS.strip() if settings.SEARCH_KEYWORDS else "developer"
        
        if not search_term:
            search_term = "developer"
        
        logger.info(f"🔍 Remotive: Fetching with search term='{search_term}'")
        
        params = {
            "search": search_term
        }

        response = get_with_retry(self.URL, params=params)

        if not response:
            logger.warning("Remotive: Failed to fetch with configured search term")
            return []

        try:
            data = response.json()
            jobs = data.get("jobs", [])

            logger.info(f"✅ Remotive: Fetched {len(jobs)} jobs with search='{search_term}'")
            
            # If we got very few results, try with a broader search
            if len(jobs) < 5 and search_term != "developer":
                logger.debug(f"Remotive: Got only {len(jobs)} jobs, trying broader search with 'developer'")
                
                fallback_params = {"search": "developer"}
                fallback_response = get_with_retry(self.URL, params=fallback_params)
                
                if fallback_response:
                    try:
                        fallback_data = fallback_response.json()
                        fallback_jobs = fallback_data.get("jobs", [])
                        logger.info(f"✅ Remotive fallback: Fetched {len(fallback_jobs)} jobs with search='developer'")
                        jobs.extend(fallback_jobs)
                    except Exception as e:
                        logger.warning(f"Remotive: Fallback search failed: {e}")
            
            return jobs
            
        except Exception as e:
            logger.error(f"Remotive: Failed to parse response: {e}")
            logger.debug(f"Raw response: {response.text[:500]}")
            return []

    def normalize(self, raw: Dict[str, Any]) -> Job:
        """Normalize Remotive job data to Job object."""
        # Extract real posted_at from API response
        publication_date_str = raw.get("publication_date")
        if publication_date_str:
            try:
                # Parse ISO format, handling timezone-aware datetimes
                posted_at = datetime.fromisoformat(publication_date_str)
                # Convert to naive UTC if timezone-aware
                if posted_at.tzinfo is not None:
                    posted_at = posted_at.replace(tzinfo=None)
            except (ValueError, TypeError):
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