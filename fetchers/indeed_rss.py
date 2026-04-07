import feedparser
from datetime import datetime
from typing import List, Dict, Any
from fetchers.base import BaseJobSource
from pipeline.models import Job
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class IndeedRSSFetcher(BaseJobSource):
    """Fetcher for Indeed RSS feed."""

    BASE_URL = "https://in.indeed.com/rss"

    def fetch(self) -> List[Dict[str, Any]]:
        """Fetch jobs from Indeed RSS feed."""
        params = {
            "q": settings.SEARCH_KEYWORDS.replace(" ", "+"),
            "l": "India"
        }
        url = f"{self.BASE_URL}?q={params['q']}&l={params['l']}"

        feed = feedparser.parse(url)

        if feed.bozo:
            logger.warning(f"Indeed RSS: Feed parsing issue: {feed.bozo_exception}")

        logger.debug(f"Indeed RSS: Fetched {len(feed.entries)} raw jobs")
        return feed.entries

    def normalize(self, raw: Dict[str, Any]) -> Job:
        """Normalize Indeed RSS entry to Job object."""
        # Extract real posted_at from RSS
        published_parsed = raw.get("published_parsed")
        if published_parsed:
            try:
                # published_parsed is a time.struct_time
                posted_at = datetime(*published_parsed[:6])
            except (ValueError, TypeError):
                logger.warning(f"Invalid date for Indeed job: {raw.get('id')}")
                posted_at = datetime.utcnow()
        else:
            posted_at = datetime.utcnow()

        return Job(
            job_id=raw.get("id", raw.get("link", "")),
            title=raw.get("title", ""),
            company=raw.get("author", "Unknown"),
            location="India",
            description=raw.get("summary", ""),
            url=raw.get("link", ""),
            source="indeed",
            posted_at=posted_at,
            fetched_at=datetime.utcnow()
        )