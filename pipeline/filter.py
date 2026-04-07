from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from config.settings import settings
from config.user_profile import USER_PROFILE
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def is_recent(job: 'Job') -> bool:
    """
    Check if the job is within the maximum age limit.

    Args:
        job: Job instance

    Returns:
        True if recent, False otherwise
    """
    max_age = timedelta(hours=settings.MAX_JOB_AGE_HOURS)
    now = datetime.now(timezone.utc)

    # Ensure posted_at is timezone-aware
    posted_at = job.posted_at
    if posted_at.tzinfo is None:
        posted_at = posted_at.replace(tzinfo=timezone.utc)

    return (now - posted_at) <= max_age


def passes_filter(job: 'Job') -> bool:
    """
    Check if the job passes all filters: recency, skills, roles, and rejection criteria.

    Args:
        job: Job instance

    Returns:
        True if passes, False otherwise
    """
    # Recency check
    if not is_recent(job):
        logger.debug(f"Job filtered out (not recent): {job.title} at {job.company}")
        return False

    # Skill matching: at least one core skill in description or title
    job_text = (job.title + " " + job.description).lower()
    has_core_skill = any(skill.lower() in job_text for skill in USER_PROFILE.core_skills)
    if not has_core_skill:
        logger.debug(f"Job filtered out (no core skills): {job.title} at {job.company}")
        return False

    # Role matching: title should match preferred roles
    title_lower = job.title.lower()
    has_preferred_role = any(role in job_text for role in USER_PROFILE.preferred_roles)
    if not has_preferred_role:
        logger.debug(f"Job filtered out (not preferred role): {job.title} at {job.company}")
        return False

    # Reject senior roles
    senior_keywords = ["senior", "lead", "principal", "staff", "architect"]
    is_senior = any(keyword in title_lower for keyword in senior_keywords)
    if is_senior:
        logger.debug(f"Job filtered out (senior role): {job.title} at {job.company}")
        return False

    return True