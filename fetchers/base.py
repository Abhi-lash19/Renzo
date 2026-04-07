from abc import ABC, abstractmethod
from typing import List
from datetime import datetime
from pipeline.models import Job
from utils.logger import get_logger

logger = get_logger(__name__)


class BaseJobSource(ABC):
    """Abstract base class for job fetchers."""

    @abstractmethod
    def fetch(self) -> List[dict]:
        """Fetch raw job data from the source."""
        pass

    @abstractmethod
    def normalize(self, raw_job: dict) -> Job:
        """Normalize raw job data into a Job object."""
        pass

    def fetch_and_normalize(self) -> List[Job]:
        """Fetch raw jobs and normalize them with validation."""
        raw_jobs = self.fetch()

        jobs = []

        for raw in raw_jobs:
            try:
                job = self.normalize(raw)

                # Enhanced validation: skip if missing title, company, OR description
                if not job.title or not job.company or not job.description:
                    logger.debug(f"Skipping job due to missing required fields: title={bool(job.title)}, company={bool(job.company)}, description={bool(job.description)}")
                    continue

                # Normalization: strip fields and lowercase description
                job.title = job.title.strip()
                job.company = job.company.strip()
                job.location = job.location.strip()
                job.description = job.description.strip().lower()
                job.url = job.url.strip() if job.url else ""

                jobs.append(job)

            except Exception as e:
                logger.warning(f"Normalization failed for job: {e}")
                continue

        return jobs