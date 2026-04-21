from typing import TYPE_CHECKING, Tuple

from utils.logger import get_logger
from utils.matching_engine import apply_match_data, build_match_data

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

DEFAULT_MIN_SKILL_THRESHOLD = 1.0
FALLBACK_MIN_SKILL_THRESHOLD = 0.6
STRONG_KEYWORD_THRESHOLD = 1.5
STRONG_RECENCY_THRESHOLD = 0.7


def _skill_score_weighted(match_data: dict) -> float:
    skill_score_raw = float(match_data.get("skill_score_raw", 0.0) or 0.0)
    skill_max_score = float(match_data.get("skill_max_score", 0.0) or 0.0)
    if skill_max_score <= 0.0:
        return 0.0
    return min(skill_score_raw / skill_max_score, 1.0)


def passes_filter(job: "Job", profile: dict, threshold: int = 4) -> Tuple[bool, str, float]:
    """
    Returns (passed, reason, filter_score).

    Filtering is match_data-driven and fails fast if match_data cannot be built.
    """
    try:
        if not getattr(job, "title", None) or not getattr(job, "description", None):
            logger.info(
                f"[FILTER_DECISION] job_id={getattr(job, 'job_id', 'unknown')} "
                f"passed=False reason=missing_title_or_description filter_score=0.0"
            )
            return False, "missing title/description", 0.0

        match_data = getattr(job, "match_data", None) or build_match_data(job, profile)
        if not match_data:
            raise ValueError("match_data must exist before filtering")

        apply_match_data(job, match_data)

        min_skill_threshold = DEFAULT_MIN_SKILL_THRESHOLD if threshold >= 4 else FALLBACK_MIN_SKILL_THRESHOLD
        skill_score_raw = float(match_data.get("skill_score_raw", 0.0) or 0.0)
        role_match_score = float(match_data.get("role_match_score", 0.0) or 0.0)
        keyword_score = float(match_data.get("keyword_score", 0.0) or 0.0)
        recency_score = float(match_data.get("recency_score", 0.0) or 0.0)
        matched_skills = match_data.get("matched_skills", []) or []
        excluded = bool(match_data.get("excluded", False))

        skill_score_weighted = _skill_score_weighted(match_data)
        filter_score = round(skill_score_weighted + role_match_score + keyword_score, 4)

        strong_skill_match = len(matched_skills) >= 2
        base_pass = (
            (skill_score_raw > min_skill_threshold)
            or (role_match_score >= 0.5)
            or strong_skill_match
        )
        boosted_pass = (
            keyword_score >= STRONG_KEYWORD_THRESHOLD
            and recency_score >= STRONG_RECENCY_THRESHOLD
            and role_match_score >= 0.4
        )

        if excluded:
            reason = "excluded by rigid constraints"
            passed = False
        elif role_match_score == 0.0 and skill_score_raw <= min_skill_threshold and not strong_skill_match:
            reason = "role mismatch and weak skill evidence"
            passed = False
        elif base_pass or boosted_pass:
            if boosted_pass and not base_pass:
                reason = "passed via keyword and recency boost"
            elif role_match_score >= 0.5:
                reason = "passed via role match confidence"
            elif strong_skill_match:
                reason = "passed via strong matched skill overlap"
            else:
                reason = "passed via weighted skill threshold"
            passed = True
        else:
            reason = "insufficient match_data evidence"
            passed = False

        logger.info(
            f"[FILTER_DECISION] job_id={getattr(job, 'job_id', 'unknown')} "
            f"passed={passed} reason={reason} filter_score={filter_score} "
            f"skill_score_raw={skill_score_raw} role_match_score={role_match_score} "
            f"keyword_score={keyword_score} recency_score={recency_score} "
            f"matched_skills={matched_skills}"
        )
        return passed, reason, filter_score
    except Exception as error:
        logger.exception(f"Error filtering job: {error}")
        return False, "fatal error during filtering", 0.0
