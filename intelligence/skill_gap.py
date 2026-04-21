from typing import Dict, Any, List
from utils.logger import get_logger
from utils.matching_engine import apply_match_data

logger = get_logger(__name__)

def compute_skill_gap(job, profile: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Compute skill gap using match_data from the matching engine (single source of truth).
    Falls back only to already-projected job fields for backward compatibility.
    """
    _ = profile

    try:
        match_data = getattr(job, "match_data", None)

        if match_data:
            apply_match_data(job, match_data)
        else:
            logger.warning(
                f"[SKILL_GAP] match_data missing for job_id={getattr(job, 'job_id', 'unknown')}, "
                f"falling back to existing projected job fields"
            )
            job.skills = sorted(set(getattr(job, "skills", []) or []))
            job.detected_skills = list(job.skills)
            job.missing_skills = sorted(set(getattr(job, "missing_skills", []) or []))

        return {
            "matched_skills": job.skills,
            "missing_skills": job.missing_skills,
        }
    except Exception as e:
        logger.exception(f"Error computing gap: {e}")
        job.skills = []
        job.missing_skills = []
        return {"matched_skills": [], "missing_skills": []}
