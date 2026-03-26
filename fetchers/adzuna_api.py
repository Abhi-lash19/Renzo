from datetime import datetime
from fetchers.base import BaseJobSource
from pipeline.models import Job
from config.settings import settings
from utils.http_client import get_with_retry


class AdzunaFetcher(BaseJobSource):

    BASE_URL = "https://api.adzuna.com/v1/api/jobs/in/search/1"

    def fetch(self):
        params = {
            "app_id": settings.ADZUNA_APP_ID,
            "app_key": settings.ADZUNA_API_KEY,
            "results_per_page": 50,
            "what": "python developer"
        }

        response = get_with_retry(self.BASE_URL, params=params)

        if not response:
            return []

        return response.json().get("results", [])

    def normalize(self, raw):
        return Job(
            job_id=str(raw.get("id")),
            title=raw.get("title"),
            company=raw.get("company", {}).get("display_name"),
            location=raw.get("location", {}).get("display_name"),
            description=raw.get("description", ""),
            url=raw.get("redirect_url"),
            source="adzuna",
            posted_at=datetime.utcnow(),
            fetched_at=datetime.utcnow()
        )