from datetime import datetime
from fetchers.base import BaseJobSource
from pipeline.models import Job
from utils.http_client import get_with_retry


class RemotiveFetcher(BaseJobSource):

    URL = "https://remotive.com/api/remote-jobs"

    def fetch(self):
        response = get_with_retry(self.URL)

        if not response:
            return []

        return response.json().get("jobs", [])

    def normalize(self, raw):
        return Job(
            job_id=str(raw.get("id")),
            title=raw.get("title"),
            company=raw.get("company_name"),
            location="Remote",
            description=raw.get("description", ""),
            url=raw.get("url"),
            source="remotive",
            posted_at=datetime.utcnow(),
            fetched_at=datetime.utcnow()
        )