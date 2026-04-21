from typing import TYPE_CHECKING, Any, Dict

from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(value, maximum))


def _get_match_data(job: "Job") -> Dict[str, Any]:
    match_data = getattr(job, "match_data", None)
    if not match_data:
        logger.error(
            f"[SCORER_ERROR] Missing match_data before scoring "
            f"job_id={getattr(job, 'job_id', 'unknown')}"
        )
        raise ValueError("match_data must be built before scoring")
    return match_data


def calculate_skill_score(job: "Job") -> float:
    try:
        match_data = _get_match_data(job)
        skill_score_raw = float(match_data.get("skill_score_raw", 0.0) or 0.0)
        skill_max_score = float(match_data.get("skill_max_score", 0.0) or 0.0)

        if skill_max_score <= 0.0:
            skill_overlap = float(match_data.get("skill_overlap", 0) or 0.0)
            normalized_skill_count = len(match_data.get("normalized_skills", []) or [])
            if skill_overlap <= 0.0 or normalized_skill_count <= 0:
                return 0.0
            return _clamp(skill_overlap / float(normalized_skill_count))

        return _clamp(skill_score_raw / skill_max_score)
    except Exception as error:
        logger.exception(f"Error in skill score calculation: {error}")
        return 0.0


def calculate_recency_score(job: "Job") -> float:
    try:
        return _clamp(float(_get_match_data(job).get("recency_score", 0.0) or 0.0))
    except Exception as error:
        logger.exception(f"Error in recency score: {error}")
        return 0.0


def calculate_role_score(job: "Job") -> float:
    try:
        match_data = _get_match_data(job)
        if match_data.get("excluded", False):
            return 0.0
        return _clamp(float(match_data.get("role_match_score", 0.0) or 0.0))
    except Exception as error:
        logger.exception(f"Error in role score: {error}")
        return 0.0


def calculate_keyword_score(job: "Job") -> float:
    try:
        match_data = _get_match_data(job)
        keyword_score = float(match_data.get("keyword_score", 0.0) or 0.0)
        keyword_max_score = float(match_data.get("keyword_max_score", 0.0) or 0.0)
        if keyword_max_score <= 0.0:
            return 0.0
        return _clamp(keyword_score / keyword_max_score)
    except Exception as error:
        logger.exception(f"Error in keyword score: {error}")
        return 0.0


def calculate_bonus_score(job: "Job") -> float:
    try:
        match_data = _get_match_data(job)
        bonus_score = float(match_data.get("bonus_score", 0.0) or 0.0)
        bonus_max_score = float(match_data.get("bonus_max_score", 0.0) or 0.0)
        if bonus_max_score <= 0.0:
            return 0.0
        return _clamp(bonus_score / bonus_max_score)
    except Exception as error:
        logger.exception(f"Error in bonus score: {error}")
        return 0.0


def calculate_learning_score(job: "Job") -> float:
    try:
        return float(_get_match_data(job).get("learning_score", 0.0) or 0.0)
    except Exception as error:
        logger.exception(f"Error in learning score: {error}")
        return 0.0


def calculate_focus_boost(job: "Job") -> int:
    try:
        focus_boost = int(_get_match_data(job).get("focus_boost", 0) or 0)
        return min(max(focus_boost, 0), 1)
    except Exception:
        return 0


def score_job(job: "Job", profile: Dict[str, Any]) -> float:
    _ = profile

    try:
        match_data = _get_match_data(job)

        skill_score = calculate_skill_score(job)
        recency_score = calculate_recency_score(job)
        role_score = calculate_role_score(job)
        keyword_score = calculate_keyword_score(job)
        bonus_score = calculate_bonus_score(job)
        learning_score = calculate_learning_score(job)
        focus_boost = calculate_focus_boost(job)

        raw_score = (
            (skill_score * 0.45) +
            (recency_score * 0.25) +
            (role_score * 0.15) +
            (keyword_score * 0.10) +
            (bonus_score * 0.05)
        )
        raw_score += learning_score

        scaled_score = _clamp(raw_score) * 10.0
        job.score = round(min(max(scaled_score + focus_boost, 0.0), 10.0), 2)
        job.score_breakdown = {
            "skill_score": round(skill_score, 4),
            "recency_score": round(recency_score, 4),
            "role_score": round(role_score, 4),
            "keyword_score": round(keyword_score, 4),
            "bonus_score": round(bonus_score, 4),
            "learning_score": round(learning_score, 4),
            "focus_boost": focus_boost,
            "base_score": round(min(max(scaled_score, 0.0), 10.0), 2),
            "skill_score_raw": round(float(match_data.get("skill_score_raw", 0.0) or 0.0), 4),
            "skill_max_score": round(float(match_data.get("skill_max_score", 0.0) or 0.0), 4),
            "keyword_score_raw": round(float(match_data.get("keyword_score", 0.0) or 0.0), 4),
            "keyword_max_score": round(float(match_data.get("keyword_max_score", 0.0) or 0.0), 4),
            "role_match_score": round(float(match_data.get("role_match_score", 0.0) or 0.0), 4),
        }

        logger.info(
            f"[SCORE_BREAKDOWN] job_id={getattr(job, 'job_id', 'unknown')} "
            f"score={job.score} breakdown={job.score_breakdown}"
        )

    except Exception as error:
        logger.exception(f"Error scoring job: {error}")
        job.score = 0.0
        job.score_breakdown = {
            "skill_score": 0.0,
            "recency_score": 0.0,
            "role_score": 0.0,
            "keyword_score": 0.0,
            "bonus_score": 0.0,
            "focus_boost": 0,
            "base_score": 0.0,
            "skill_score_raw": 0.0,
            "skill_max_score": 0.0,
            "keyword_score_raw": 0.0,
            "keyword_max_score": 0.0,
            "role_match_score": 0.0,
            "learning_score": 0.0,
        }

    return job.score
