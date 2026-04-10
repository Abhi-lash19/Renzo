from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Tuple

from config.settings import settings
from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

SENIORITY_TERMS = ("senior", "lead", "principal")


def is_recent(job: "Job") -> bool:
    try:
        max_age = timedelta(hours=settings.MAX_JOB_AGE_HOURS)
        if not getattr(job, "posted_at", None):
            return True
        now = datetime.utcnow()
        posted_at = job.posted_at.replace(tzinfo=None) if job.posted_at.tzinfo else job.posted_at
        age = now - posted_at
        return age <= max_age
    except Exception:
        return True


def _has_any_term(text: str, terms: list[str]) -> bool:
    return any(term and contains_term(text, term) for term in terms)


def passes_filter(job: "Job", profile: dict, fallback: bool = False) -> Tuple[bool, str]:
    try:
        if not job.title or not job.company or not job.description:
            return False, "invalid job data"

        if not is_recent(job):
            return False, "job is older than recency threshold"

        title_text = normalize_text(job.title)
        job_text = normalize_text(f"{job.title} {job.description}")

        if _has_any_term(title_text, list(SENIORITY_TERMS)):
            return False, "seniority keyword in title"

        for keyword in profile.get("exclude_keywords", []):
            if keyword and contains_term(job_text, keyword):
                return False, f"exclude keyword '{keyword}'"

        core_skill_match = _has_any_term(job_text, profile.get("core_skills", []))
        role_match = _has_any_term(title_text, profile.get("preferred_roles", []))

        if core_skill_match and role_match:
            return True, "accepted via core skill and role match"
        if core_skill_match:
            return True, "accepted via core skill match"
        if role_match:
            return True, "accepted via role match"

        if fallback and profile.get("is_empty", False):
            keyword_match = _has_any_term(job_text, profile.get("preferred_keywords", []))
            if keyword_match:
                return True, "accepted via fallback keyword match"

        return False, "no alignment signals allowed"
    except Exception as e:
        logger.exception(f"Error filtering job: {e}")
        return False, "fatal error during filtering"
