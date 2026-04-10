from datetime import datetime, timedelta
from typing import Dict, List, Set, TYPE_CHECKING
from intelligence.skill_extractor import extract_skills
from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def _normalize_skill_set(skills: List[str]) -> Set[str]:
    return {
        normalize_text(skill)
        for skill in skills
        if skill and skill.strip()
    }


def calculate_skill_score(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate a weighted skill match score using core and secondary priority."""
    core_skills = _normalize_skill_set(profile.get("core_skills", []))
    secondary_skills = _normalize_skill_set(profile.get("secondary_skills", []))
    job_skills = _normalize_skill_set(job.skills or [])

    if not core_skills and not secondary_skills:
        return 0.0

    core_matches = len(job_skills & core_skills)
    secondary_matches = len(job_skills & secondary_skills)

    core_weight = 1.0
    secondary_weight = 0.5
    total_possible = len(core_skills) * core_weight + len(secondary_skills) * secondary_weight

    if total_possible == 0:
        return 0.0

    score = (core_matches * core_weight + secondary_matches * secondary_weight) / total_possible
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
    title_text = normalize_text(job.title)

    for role in profile.get("preferred_roles", []):
        if role and normalize_text(role) == title_text:
            return 1.0

    for role in profile.get("preferred_roles", []):
        if role and normalize_text(role) in title_text:
            return 0.5

    return 0.0


def calculate_alignment_score(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate alignment score from profile keywords, projects, and experience."""
    combined_text = normalize_text(" ".join(filter(None, [job.title, job.description, job.company])))
    alignment_keywords = [
        keyword
        for keyword in profile.get("projects", [])
        + profile.get("experience", [])
        + profile.get("preferred_keywords", [])
        if keyword and keyword.strip()
    ]

    if not alignment_keywords or not combined_text:
        return 0.0

    match_count = 0
    for keyword in alignment_keywords:
        if contains_term(combined_text, keyword):
            match_count += 1

    return min(match_count / len(alignment_keywords), 1.0)


def calculate_bonus_score(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Calculate bonus score from profile bonus keywords."""
    combined_text = normalize_text(" ".join(filter(None, [job.title, job.company, job.description])))
    bonus_keywords = [kw for kw in profile.get("bonus_keywords", []) if kw and kw.strip()]

    if not bonus_keywords or not combined_text:
        return 0.0

    matches = sum(1 for keyword in bonus_keywords if normalize_text(keyword) in combined_text)
    return min(matches * 0.025, 0.05)


def score_job(job: 'Job', profile: Dict[str, List[str]]) -> float:
    """Compute a job score using precomputed skills and profile signals."""
    if not job.skills:
        extract_skills(job, profile)

    skill_score = calculate_skill_score(job, profile)
    recency_score = calculate_recency_score(job)
    role_score = calculate_role_score(job, profile)
    alignment_score = calculate_alignment_score(job, profile)
    bonus_score = calculate_bonus_score(job, profile)

    raw_score = (
        (skill_score * 0.4) +
        (recency_score * 0.2) +
        (role_score * 0.15) +
        (alignment_score * 0.2) +
        bonus_score
    )

    job.score = min(max(raw_score * 10.0, 0.0), 10.0)

    logger.debug(
        f"[SCORER] job_id={getattr(job, 'job_id', 'unknown')} "
        f"score={job.score:.2f} skill={skill_score:.2f} recency={recency_score:.2f} "
        f"role={role_score:.2f} alignment={alignment_score:.2f} bonus={bonus_score:.2f}"
    )
    return job.score
