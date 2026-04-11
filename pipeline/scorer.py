from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text
from utils.matching_engine import build_match_data, get_profile_list

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

def get_job_age_hours(job: 'Job') -> float:
    try:
        if not getattr(job, "posted_at", None):
            return 0.0
        posted_at = job.posted_at.replace(tzinfo=None) if job.posted_at.tzinfo else job.posted_at
        return max((datetime.utcnow() - posted_at).total_seconds() / 3600.0, 0.0)
    except Exception:
        return 0.0

def calculate_skill_score(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        matched_skills = set(job.match_data["matched_skills"])
        detected_skills = set(getattr(job, "detected_skills", []) or [])
        skill_signals = detected_skills | matched_skills
        if not skill_signals:
            return 0.0

        weighted_skills = {
            normalize_text(skill): weight
            for skill, weight in profile.get("weighted_skills", {}).items()
            if normalize_text(skill)
        }
        if not weighted_skills:
            weighted_skills = {
                normalize_text(skill): 1.0
                for skill in profile.get("all_skills", [])
                if normalize_text(skill)
            }

        if not weighted_skills:
            return 0.0

        matched_weight = sum(weighted_skills.get(skill, 0.0) for skill in skill_signals if skill in weighted_skills)
        target_weight = max(min(sum(weighted_skills.values()), 6.0), 1.0)
        return min(matched_weight / target_weight, 1.0)
    except Exception as e:
        logger.exception(f"Error in skill score calculation: {e}")
        return 0.0

def calculate_recency_score(job: 'Job') -> float:
    try:
        age_hours = get_job_age_hours(job)
        if age_hours < 24:
            return 1.0
        if age_hours < 72:
            return 0.7
        if age_hours < 168:
            return 0.4
        return 0.0
    except Exception as e:
        logger.exception(f"Error in recency score: {e}")
        return 0.0

def calculate_role_score(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        if job.match_data["excluded"]:
            return 0.0

        title = normalize_text(getattr(job, "title", ""))
        job_text = normalize_text(f"{getattr(job, 'title', '')} {getattr(job, 'description', '')}")

        # If matching engine says False, strictly limit base role score.
        if not job.match_data["role_match"]:
            return 0.35

        target_roles = get_profile_list(profile, "target_roles")
        if contains_term(title, "backend") or contains_term(title, "python"):
            return 1.0
        if any(term and contains_term(title, term) for term in target_roles):
            return 0.9
            
        matched_skills = set(job.match_data["matched_skills"])
        detected_skills = set(getattr(job, "detected_skills", []) or [])
        skill_signals = detected_skills | matched_skills
        
        if contains_term(title, "software engineer") and skill_signals.intersection({"backend", "python", "api"}):
            return 0.85
        if any(term and contains_term(job_text, term) for term in ["backend", "python", "api", "microservices"]):
            return 0.75
            
        # Role matches, but implicitly via description fallback 
        return 0.65
    except Exception as e:
        logger.exception(f"Error in role score: {e}")
        return 0.35

def calculate_bonus_score(job: 'Job') -> float:
    try:
        score = 0.0
        title = normalize_text(getattr(job, "title", ""))
        location = normalize_text(getattr(job, "location", ""))
        if contains_term(title, "remote") or contains_term(location, "remote") or getattr(job, "is_remote", False):
            score += 1.0
        if getattr(job, "is_startup", False):
            score += 1.0
        return score
    except Exception:
        return 0.0

def calculate_focus_boost(job: 'Job', profile: Dict[str, Any]) -> int:
    try:
        searchable_text = normalize_text(
            f"{getattr(job, 'title', '')} {getattr(job, 'description', '')} {getattr(job, 'location', '')}"
        )
        matched_skills = set(job.match_data["matched_skills"])
        detected_skills = set(getattr(job, "detected_skills", []) or [])
        skill_signals = detected_skills | matched_skills
        
        boost = 0
        bonus_skills = get_profile_list(profile, "bonus_keywords")
        for skill in bonus_skills:
            if skill in skill_signals or contains_term(searchable_text, skill):
                boost += 1
        return boost
    except Exception:
        return 0

def score_job(job: 'Job', profile: Dict[str, Any]) -> float:
    try:
        # Guarantee match_data exists identical to filter.
        build_match_data(job, profile)
        
        skill_score = calculate_skill_score(job, profile)
        recency_score = calculate_recency_score(job)
        role_score = calculate_role_score(job, profile)
        bonus_score = calculate_bonus_score(job)
        focus_boost = calculate_focus_boost(job, profile)

        raw_score = (
            (skill_score * 0.5) +
            (recency_score * 0.3) +
            (role_score * 0.15) +
            (bonus_score * 0.05)
        )

        scaled_score = raw_score * 10.0
        job.score = round(min(max(scaled_score + focus_boost, 0.0), 10.0), 2)
        job.score_breakdown = {
            "skill_score": round(skill_score, 4),
            "recency_score": round(recency_score, 4),
            "role_score": round(role_score, 4),
            "bonus_score": round(bonus_score, 4),
            "focus_boost": focus_boost,
            "base_score": round(min(max(scaled_score, 0.0), 10.0), 2),
        }

    except Exception as e:
        logger.exception(f"Error scoring job: {e}")
        job.score = 0.0
        job.score_breakdown = {
            "skill_score": 0.0,
            "recency_score": 0.0,
            "role_score": 0.0,
            "bonus_score": 0.0,
            "focus_boost": 0,
            "base_score": 0.0,
        }

    return job.score
