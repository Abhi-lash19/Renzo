from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Tuple
from config.settings import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def is_recent(job: 'Job') -> bool:
    """
    Check if the job is within the maximum age limit.
    """
    max_age_hours = settings.MAX_JOB_AGE_HOURS

    if max_age_hours < 24:
        max_age = timedelta(days=7)
    else:
        max_age = timedelta(hours=max_age_hours)

    now = datetime.utcnow()

    posted_at = job.posted_at
    if posted_at.tzinfo is not None:
        posted_at = posted_at.replace(tzinfo=None)

    age = now - posted_at

    logger.debug(f"[FILTER] Job age: {age}, Max allowed: {max_age}")

    return age <= max_age


def passes_filter(job: 'Job', profile: dict, fallback: bool = False) -> Tuple[bool, str]:
    """
    Check if the job passes all filters using a dynamic profile.
    """
    logger.debug(f"[FILTER] Evaluating: {job.title} at {job.company}")

    if not job.title or not job.company or not job.description:
        return False, "invalid job data"

    if not is_recent(job):
        logger.debug(f"[FILTER] ⏰ Old job (allowed): {job.title}")

    job_text = f"{job.title} {job.description}".lower()

    for keyword in profile.get("exclude_keywords", []):
        if keyword and keyword in job_text:
            logger.debug(f"[FILTER] ❌ Rejected by exclude keyword: {keyword}")
            return False, f"exclude keyword '{keyword}'"

    profile_skills = [skill for skill in profile.get("core_skills", []) if skill] + [skill for skill in profile.get("secondary_skills", []) if skill]
    matched_skills = [skill for skill in profile_skills if skill in job_text]

    if matched_skills:
        logger.debug(f"[FILTER] ✅ Matched skills: {matched_skills[:5]}")
    else:
        if fallback:
            fallback_terms = ["developer", "engineer", "software", "backend", "full stack", "fullstack"]
            if any(term in job_text for term in fallback_terms):
                logger.debug(f"[FILTER] ✅ Fallback accepted by broader keyword in: {job.title}")
            else:
                logger.debug(f"[FILTER] ❌ No matching skills found")
                return False, "no matching skills"
        else:
            logger.debug(f"[FILTER] ❌ No matching skills found")
            return False, "no matching skills"

    matched_roles = [role for role in profile.get("preferred_roles", []) if role and role in job_text]
    if matched_roles:
        logger.debug(f"[FILTER] ✅ Preferred role matched: {matched_roles[:3]}")
    else:
        logger.debug(f"[FILTER] ℹ️ No preferred role matched (still accepted): {job.title}")

    title_lower = job.title.lower()
    senior_keywords = ["senior", "lead", "principal", "staff", "architect"]
    if any(keyword in title_lower for keyword in senior_keywords):
        logger.debug(f"[FILTER] ❌ Rejected by senior-level keyword in title: {title_lower}")
        return False, "senior role"

    logger.debug(f"[FILTER] ✅ Accepted: {job.title}")
    return True, "accepted"
