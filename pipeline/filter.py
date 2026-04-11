from datetime import datetime
from typing import TYPE_CHECKING, Tuple

from utils.logger import get_logger
from utils.matching_engine import build_match_data

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def get_job_age_hours(job: "Job") -> float:
    try:
        if not getattr(job, "posted_at", None):
            return 0.0
        now = datetime.utcnow()
        posted_at = job.posted_at.replace(tzinfo=None) if job.posted_at.tzinfo else job.posted_at
        return max((now - posted_at).total_seconds() / 3600.0, 0.0)
    except Exception:
        return 0.0


def get_recency_score_filter(job: "Job") -> int:
    age_hours = get_job_age_hours(job)
    if age_hours <= 24:
        return 2
    if age_hours <= 72:
        return 1
    return 0


def passes_filter(job: "Job", profile: dict, threshold: int = 4) -> Tuple[bool, str, float]:
    """
    Returns (passed, reason, filter_score)
    """
    try:
        if not getattr(job, "title", None) or not getattr(job, "description", None):
            return False, "missing title/description", 0.0

        # Run unified matching
        build_match_data(job, profile)
        match_data = job.match_data

        # 1. Strict Exclusions Rule
        if match_data.get("excluded", False):
            return False, "excluded by rigid constraints", 0.0

        # 2. Extract matching signals globally
        role_match = match_data.get("role_match", False)
        matched_skills = match_data.get("matched_skills", [])
        skill_match_count = match_data.get("skill_overlap", 0)
        strong_skill_match = skill_match_count >= 2

        # 3. Reject if neither role nor skills aligned
        if not (role_match or strong_skill_match):
            return False, "missing target role match and strong skill match", 0.0

        # 4. Generate Signal Score
        keyword_match_count = len(match_data.get("matched_keywords", []))
        recency_score_val = get_recency_score_filter(job)

        filter_score = (
            (1 if role_match else 0) * 3 +
            min(skill_match_count, 3) * 2 +
            min(keyword_match_count, 3) +
            recency_score_val
        )

        logger.debug(
            f"[FILTER_DEBUG] title={job.title[:40]} | "
            f"role_match={role_match} skill_count={skill_match_count} "
            f"keyword_count={keyword_match_count} recency={recency_score_val} "
            f"filter_score={filter_score}"
        )

        if filter_score >= threshold:
            return True, f"passed (score {filter_score})", float(filter_score)

        return False, f"score {filter_score} below threshold {threshold}", float(filter_score)
    except Exception as e:
        logger.exception(f"Error filtering job: {e}")
        return False, "fatal error during filtering", 0.0
