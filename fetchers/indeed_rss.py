import feedparser
from datetime import datetime
from fetchers.base import BaseJobSource
from pipeline.models import Job
from utils.logger import get_logger

logger = get_logger(__name__)


class IndeedRSSFetcher(BaseJobSource):

    URL = "https://in.indeed.com/rss?q=python+developer&l=India"

    def fetch(self):
        feed = feedparser.parse(self.URL)

        if feed.bozo:
            logger.warning("RSS feed parsing issue")

        return feed.entries

    def normalize(self, raw):
        return Job(
            job_id=raw.get("id", raw.get("link")),
            title=raw.get("title"),
            company=raw.get("author", "Unknown"),
            location="India",
            description=raw.get("summary", ""),
            url=raw.get("link"),
            source="indeed",
            posted_at=datetime.utcnow(),
            fetched_at=datetime.utcnow()
        )