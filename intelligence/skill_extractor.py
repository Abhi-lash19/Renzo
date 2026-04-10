import re
from typing import Dict, List, Set
from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

logger = get_logger(__name__)


def extract_skills(job, profile: Dict[str, List[str]]) -> Set[str]:
    """Extract skills from job text using token-safe matching against profile keywords."""
    raw_text = " ".join(filter(None, [job.title, job.description]))
    normalized_text = normalize_text(raw_text)

    if not normalized_text:
        job.skills = []
        return set()

    profile_keywords = [
        skill.strip().lower()
        for skill in profile.get("core_skills", []) + profile.get("secondary_skills", [])
        if skill and skill.strip()
    ]

    matched: Set[str] = set()
    seen: Set[str] = set()

    for skill in profile_keywords:
        if skill in seen:
            continue
        seen.add(skill)
        if contains_term(normalized_text, skill):
            matched.add(skill)

    job.skills = sorted(matched)
    logger.debug(
        f"[SKILL_EXTRACTOR] job_id={getattr(job, 'job_id', 'unknown')} "
        f"title={job.title or 'Unknown'} skills={job.skills}"
    )
    return matched
