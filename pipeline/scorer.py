from datetime import datetime
from typing import Any, Dict, TYPE_CHECKING

from utils.logger import get_logger
from utils.text_utils import contains_term

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

def calculate_skill_score(job: 'Job', profile: Dict[str, Any]) -> float:
    if not job.skills:
        return 0.0
    try:
        weighted_skills = profile.get("weighted_skills", {})
        matched_weight = sum(weighted_skills.get(skill, 0.0) for skill in job.skills)
        total_weight = sum(weighted_skills.values())
        
        if total_weight == 0.0:
            return 0.0
            
        return min(matched_weight / total_weight, 1.0)
    except Exception as e:
        logger.exception(f"Error in skill score calculation: {e}")
        return 0.0


def calculate_recency_score(job: 'Job') -> float:
    try:
        if not getattr(job, "posted_at", None):
            return 0.5
        posted_at = job.posted_at.replace(tzinfo=None) if job.posted_at.tzinfo else job.posted_at
        age_hours = max((datetime.utcnow() - posted_at).total_seconds() / 3600.0, 0.0)
        if age_hours <= 6:
            return 1.0
        if age_hours <= 12:
            return 0.8
        if age_hours <= 24:
            return 0.6
        if age_hours <= 48:
            return 0.3
        return 0.1
    except Exception as e:
        logger.exception(f"Error in recency score calculation: {e}")
        return 0.1


def calculate_role_score(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        title = str(getattr(job, "title", "") or "")
        roles = profile.get("preferred_roles", [])
        if any(role and contains_term(title, role) for role in roles):
            return 1.0
        preferred_keywords = profile.get("preferred_keywords", [])
        if any(keyword and contains_term(title, keyword) for keyword in preferred_keywords):
            return 0.6
        return 0.2
    except Exception as e:
        logger.exception(f"Error in role score calculation: {e}")
        return 0.2


def calculate_startup_bonus(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        text = f"{getattr(job, 'title', '')} {getattr(job, 'description', '')}"
        if getattr(job, "is_startup", False):
            return 1.0
        if any(term and contains_term(text, term) for term in profile.get("bonus_keywords", [])):
            return 1.0
        return 0.0
    except Exception as e:
        logger.exception(f"Error in startup bonus calculation: {e}")
        return 0.0

def score_job(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        skill_score = calculate_skill_score(job, profile)
        recency_score = calculate_recency_score(job)
        role_score = calculate_role_score(job, profile)
        startup_bonus = calculate_startup_bonus(job, profile)

        raw_score = (
            (skill_score * 0.50) +
            (recency_score * 0.30) +
            (role_score * 0.15) +
            (startup_bonus * 0.05)
        )
        job.score = round(min(max(raw_score * 10.0, 0.0), 10.0), 2)
        job.score_breakdown = {
            "skill_score": round(skill_score, 4),
            "recency_score": round(recency_score, 4),
            "role_score": round(role_score, 4),
            "startup_bonus": round(startup_bonus, 4),
        }

    except Exception as e:
        logger.exception(f"Error scoring job: {e}")
        job.score = 0.1
        job.score_breakdown = {
            "skill_score": 0.0,
            "recency_score": 0.0,
            "role_score": 0.0,
            "startup_bonus": 0.0,
        }

    return job.score
