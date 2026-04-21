from datetime import datetime
from typing import List


class Job:
    """Represents a job posting."""

    def __init__(
        self,
        job_id: str,
        title: str,
        company: str,
        location: str,
        description: str,
        url: str,
        source: str,
        posted_at: datetime,
        fetched_at: datetime,
    ):
        self.job_id = job_id
        self.title = title
        self.company = company
        self.location = location
        self.description = description
        self.url = url
        self.source = source
        self.posted_at = posted_at
        self.fetched_at = fetched_at

        self.skills: List[str] = []
        self.detected_skills: List[str] = []
        self.match_data: dict = {}
        self.score: float = 0.0
        self.score_breakdown: dict = {}
        self.is_remote: bool = False
        self.is_startup: bool = False
        self.missing_skills: List[str] = []
        self.insight: dict = {}

    def __repr__(self) -> str:
        return f"<Job {self.title} at {self.company}>"
