from datetime import datetime, timedelta
from typing import Dict, List, Set, TYPE_CHECKING
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def extract_skills(job: 'Job', profile: Dict[str, List[str]]) -> Set[str]:
    """Extract matching skills from the job using profile keywords."""
    combined_text = (job.title + " " + job.description).lower()
    all_skills = [skill for skill in profile.get("core_skills", []) if skill] + [skill for skill in profile.get("secondary_skills", []) if skill]

    extracted = {skill for skill in all_skills if skill in combined_text}
    return extracted


def calculate_skill_score(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate a normalized skill score based on profile skill coverage."""
    all_profile_skills = [skill for skill in profile.get("core_skills", []) if skill] + [skill for skill in profile.get("secondary_skills", []) if skill]

    if not all_profile_skills:
        return 0.0

    matching_skills = len(job.skills)
    score = matching_skills / len(all_profile_skills)
    return min(score, 1.0)


def calculate_recency_score(job: 'Job') -> float:
    """Calculate a recency score: newer jobs score higher."""
    now = datetime.utcnow()
    age = now - job.posted_at
    max_age = timedelta(days=7)

    if age.total_seconds() < 0:
        return 1.0
    if age >= max_age:
        return 0.0

    return max(1.0 - (age.total_seconds() / max_age.total_seconds()), 0.0)


def calculate_role_score(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate role relevance score based on preferred roles."""
    title_lower = job.title.lower()

    for role in profile.get("preferred_roles", []):
        if role and role == title_lower:
            return 1.0

    for role in profile.get("preferred_roles", []):
        if role and role in title_lower:
            return 0.5

    return 0.0


def calculate_keyword_bonus(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate a small bonus for matching preferred and bonus keywords."""
    combined = (job.title + " " + job.company + " " + job.description).lower()
    keywords = [kw for kw in profile.get("preferred_keywords", []) if kw] + [kw for kw in profile.get("bonus_keywords", []) if kw]
    matches = sum(1 for keyword in keywords if keyword in combined)
    return min(matches * 0.03, 0.15)


def calculate_startup_bonus(job: 'Job') -> float:
    """Calculate startup bonus for startup-related postings."""
    startup_keywords = ["startup", "early stage", "series a", "series b", "seed", "seed stage"]
    combined = (job.title + " " + job.company + " " + job.description).lower()

    return 0.05 if any(keyword in combined for keyword in startup_keywords) else 0.0


def score_job(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Compute a job score using profile-driven relevance signals."""
    job.skills = list(extract_skills(job, profile))
    skill_score = calculate_skill_score(job, profile)
    recency_score = calculate_recency_score(job)
    role_score = calculate_role_score(job, profile)
    bonus_score = calculate_keyword_bonus(job, profile)
    startup_bonus = calculate_startup_bonus(job)

    final_score = (
        (skill_score * 0.45) +
        (recency_score * 0.25) +
        (role_score * 0.15) +
        bonus_score +
        startup_bonus
    )

    job.score = min(max(final_score, 0.0), 1.0)

    logger.debug(
        f"Scored {job.title} at {job.company}: "
        f"skill={skill_score:.2f}, recency={recency_score:.2f}, "
        f"role={role_score:.2f}, bonus={bonus_score:.2f}, "
        f"startup={startup_bonus:.2f} → {job.score:.2f}"
    )

    return job.score
