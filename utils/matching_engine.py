from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple

if TYPE_CHECKING:
    from pipeline.models import Job

from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text
from intelligence.skill_extractor import extract_skills

logger = get_logger(__name__)

# Essential fallback configuration if profile misses them
DEFAULT_CONFIG = {
    "target_roles": ["developer", "backend", "python", "software engineer", "api", "microservices"],
    "exclude_keywords": ["frontend", "ui", "react", "angular", "mobile", "ios", "android", "flutter", "sales", "marketing", "senior", "lead", "principal"],
    "bonus_keywords": ["startup", "early stage", "remote", "cloud native", "saas"],
    "preferred_keywords": ["backend", "api", "microservices", "cloud", "aws", "rest", "lambda", "serverless"]
}


def _unique_terms(*term_groups: List[str]) -> List[str]:
    seen = set()
    ordered_terms = []
    for group in term_groups:
        for term in group:
            if not term:
                continue
            normalized_term = normalize_text(term)
            if not normalized_term or normalized_term in seen:
                continue
            seen.add(normalized_term)
            ordered_terms.append(normalized_term)
    return ordered_terms


def get_profile_list(profile: Dict[str, Any], key: str) -> List[str]:
    return _unique_terms(profile.get(key, []), DEFAULT_CONFIG.get(key, []))


def has_any_term(text: str, terms: List[str]) -> bool:
    if not text or not terms:
        return False
    return any(term and contains_term(text, term) for term in terms)


def detect_exclusions(text: str, profile: Dict[str, Any]) -> bool:
    """Returns True if any explicit excluded terms are found in text."""
    exclude_terms = get_profile_list(profile, "exclude_keywords")
    return has_any_term(text, exclude_terms)


def match_roles(title: str, description: str, profile: Dict[str, Any]) -> bool:
    """Matches role leveraging strict boundaries and explicit developer fallback."""
    title_norm = normalize_text(title)
    desc_norm = normalize_text(description)
    job_text = f"{title_norm} {desc_norm}"
    
    target_roles = get_profile_list(profile, "target_roles")
    
    # 1. Direct title strict boundary match
    if has_any_term(title_norm, target_roles):
        return True
        
    # 2. Strict exact "developer" term fallback
    if "developer" in title_norm:
        return True
        
    # 3. Soft internal description match loop boundary search
    for role in target_roles:
        if role in job_text:
            return True
            
    return False


def build_match_data(job: "Job", profile: dict) -> dict:
    """
    Build standardized match data for a job.

    This is the SINGLE SOURCE OF TRUTH for:
    - skill matching
    - title similarity
    - experience alignment
    - filtering signals (excluded, role_match, keywords)
    """

    if not job:
        raise ValueError("Job cannot be None")

    if not profile:
        raise ValueError("Profile cannot be empty")

    try:
        title = job.title or ""
        description = job.description or ""

        job_text = f"{title} {description}".lower()

        # -------------------------------
        # 1. SKILL MATCHING
        # -------------------------------
        user_skills = set(profile.get("all_skills", []))
        matched_skills = set()

        for skill in user_skills:
            if skill.lower() in job_text:
                matched_skills.add(skill.lower())

        missing_skills = list(user_skills - matched_skills)
        matched_skills = list(matched_skills)
        skill_overlap = len(matched_skills)

        # -------------------------------
        # 2. ROLE MATCH
        # -------------------------------
        role_match = match_roles(title, description, profile)

        # -------------------------------
        # 3. EXCLUSION CHECK
        # -------------------------------
        excluded = detect_exclusions(job_text, profile)

        # -------------------------------
        # 4. KEYWORD MATCHING
        # -------------------------------
        preferred_keywords = get_profile_list(profile, "preferred_keywords")
        bonus_keywords = get_profile_list(profile, "bonus_keywords")

        matched_keywords = []

        for kw in preferred_keywords + bonus_keywords:
            if kw in job_text:
                matched_keywords.append(kw)

        # -------------------------------
        # 5. TITLE MATCH (for scoring phase)
        # -------------------------------
        title_norm = normalize_text(title)
        target_roles = profile.get("target_roles", [])

        title_match = 0.0
        for role in target_roles:
            if role.lower() in title_norm:
                title_match = 1.0
                break

        # -------------------------------
        # 6. EXPERIENCE (placeholder)
        # -------------------------------
        experience_match = 1.0

        match_data = {
            "matched_skills": matched_skills,
            "missing_skills": missing_skills,
            "skill_overlap": skill_overlap,

            "role_match": role_match,
            "excluded": excluded,
            "matched_keywords": matched_keywords,

            "title_match": title_match,
            "experience_match": experience_match,
        }

        job.match_data = match_data

        return match_data

    except Exception as e:
        logger.exception(
            f"[MATCH_BUILD_ERROR] job_id={getattr(job, 'job_id', 'unknown')} error={e}"
        )
        raise