from typing import Any, Dict

from utils.logger import get_logger
from utils.matching_engine import apply_match_data

logger = get_logger(__name__)


def extract_skills(job, profile: Dict[str, Any]) -> None:
    """
    Backward-compatible shim.

    Skill extraction now lives exclusively in build_match_data(); this function only
    projects existing match_data onto the job object and never recomputes skills.
    """
    _ = profile

    try:
        match_data = getattr(job, "match_data", None)
        if not match_data:
            logger.warning(
                f"[SKILL_EXTRACTOR_DEPRECATED] match_data missing for job_id="
                f"{getattr(job, 'job_id', 'unknown')}; leaving projected skills unchanged"
            )
            job.skills = list(getattr(job, "skills", []) or [])
            job.detected_skills = list(job.skills)
            job.missing_skills = list(getattr(job, "missing_skills", []) or [])
            return

        apply_match_data(job, match_data)
    except Exception as error:
        logger.exception(f"Error projecting skills from match_data: {error}")
        job.skills = list(getattr(job, "skills", []) or [])
        job.detected_skills = list(job.skills)
        job.missing_skills = list(getattr(job, "missing_skills", []) or [])
