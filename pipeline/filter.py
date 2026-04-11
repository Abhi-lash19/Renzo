from datetime import datetime
from typing import TYPE_CHECKING, Tuple

from intelligence.skill_extractor import extract_skills
from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

SENIORITY_TERMS = ["senior", "lead", "principal"]
EXCLUDE_TERMS = ["sales", "marketing"]
TARGET_ROLES = []
ROLE_EXCLUDE_TERMS = ["frontend", "react", "angular", "ui", "mobile", "ios", "android", "flutter"]


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


def _has_any_term(text: str, terms: list[str]) -> bool:
    return any(term and contains_term(text, term) for term in terms)


def _matched_terms(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if term and contains_term(text, term)]


def _unique_terms(*term_groups: list[str]) -> list[str]:
    seen = set()
    ordered_terms = []
    for group in term_groups:
        for term in group:
            normalized_term = normalize_text(term)
            if not normalized_term or normalized_term in seen:
                continue
            seen.add(normalized_term)
            ordered_terms.append(normalized_term)
    return ordered_terms


def _get_target_roles(profile: dict) -> list[str]:
    return _unique_terms(
        TARGET_ROLES,
        profile.get("target_roles", []),
        profile.get("preferred_roles", []),
    )


def _get_role_exclusions(profile: dict) -> list[str]:
    return _unique_terms(ROLE_EXCLUDE_TERMS, profile.get("exclude_keywords", []), EXCLUDE_TERMS)


def _ensure_skill_signals(job: "Job", profile: dict) -> None:
    if getattr(job, "_skill_signals_cached", False):
        return
    extract_skills(job, profile)
    setattr(job, "_skill_signals_cached", True)


def passes_filter(job: "Job", profile: dict, threshold: int = 4) -> Tuple[bool, str, float]:
    """
    Returns (passed, reason, filter_score)
    """
    try:
        if not job.title or not job.description:
            return False, "missing title/description", 0.0

        title_text = normalize_text(job.title)
        job_text = normalize_text(f"{job.title} {job.description}")

        if _has_any_term(title_text, SENIORITY_TERMS):
            return False, "seniority keyword in title", 0.0

        if _has_any_term(title_text, EXCLUDE_TERMS):
            return False, "excluded domain in title", 0.0

        role_exclusions = _get_role_exclusions(profile)
        excluded_title_terms = _matched_terms(title_text, role_exclusions)
        if excluded_title_terms:
            return False, f"excluded role keywords in title: {', '.join(excluded_title_terms[:3])}", 0.0

        _ensure_skill_signals(job, profile)

        target_roles = _get_target_roles(profile)
        detected_skills = getattr(job, "detected_skills", []) or []
        matched_skills = getattr(job, "skills", []) or []

        role_match = _has_any_term(title_text, target_roles) or _has_any_term(job_text, target_roles)
        if not role_match:
            for role in target_roles:
                if role in job_text:
                    role_match = True
                    break
        skill_match_count = max(len(matched_skills), len(detected_skills) // 2)
        strong_skill_match = skill_match_count >= 1

        detected_keywords = [
            kw
            for kw in _unique_terms(profile.get("preferred_keywords", []))
            if contains_term(job_text, kw)
        ]
        keyword_match_count = len(detected_keywords)
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

        if not (role_match or strong_skill_match):
            return False, "missing target role match and strong skill match", float(filter_score)

        if filter_score >= threshold:
            return True, f"passed (score {filter_score})", float(filter_score)

        return False, f"score {filter_score} below threshold {threshold}", float(filter_score)
    except Exception as e:
        logger.exception(f"Error filtering job: {e}")
        return False, "fatal error during filtering", 0.0
