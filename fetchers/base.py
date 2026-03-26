from abc import ABC, abstractmethod
from typing import List
from pipeline.models import Job
from utils.logger import get_logger

logger = get_logger(__name__)


class BaseJobSource(ABC):

    @abstractmethod
    def fetch(self) -> List[dict]:
        pass

    @abstractmethod
    def normalize(self, raw_job: dict) -> Job:
        pass

    def fetch_and_normalize(self) -> List[Job]:
        raw_jobs = self.fetch()

        jobs = []

        for raw in raw_jobs:
            try:
                job = self.normalize(raw)

                # Basic validation
                if not job.title or not job.company:
                    continue

                jobs.append(job)

            except Exception as e:
                logger.warning(f"Normalization failed: {e}")
                continue

        return jobs