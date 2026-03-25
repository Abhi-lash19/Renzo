from datetime import datetime

class Job:
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

        # computed later
        self.skills = []
        self.score = 0.0
        self.is_remote = False
        self.is_startup = False
        self.missing_skills = []

    def __repr__(self):
        return f"<Job {self.title} at {self.company}>"