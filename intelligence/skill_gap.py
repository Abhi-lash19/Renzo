from typing import Dict, Any, List
from utils.logger import get_logger

logger = get_logger(__name__)

def compute_skill_gap(job, profile: Dict[str, Any]) -> Dict[str, List[str]]:
    try:
        profile_skills = set(profile.get("all_skills", []))
        job_skills = set(getattr(job, "detected_skills", []) or [])

        matched = job_skills & profile_skills
        missing = job_skills - profile_skills

        job.skills = sorted(matched)
        job.missing_skills = sorted(missing)

        return {
            "matched_skills": sorted(matched),
            "missing_skills": sorted(missing),
        }
    except Exception as e:
        logger.exception(f"Error computing gap: {e}")
        job.missing_skills = []
        return {"matched_skills": [], "missing_skills": []}
