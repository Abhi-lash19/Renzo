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


def build_match_data(job: "Job", profile: Dict[str, Any]) -> None:
    """Single Source of Truth: computes matching points once and saves directly to job."""
    
    # Avoid recomputation
    if hasattr(job, "match_data") and job.match_data:
        return

    title_text = normalize_text(getattr(job, "title", ""))
    job_text = normalize_text(f"{getattr(job, 'title', '')} {getattr(job, 'description', '')}")
    
    # Strict exclusion check
    is_excluded = detect_exclusions(title_text, profile)

    # Role check
    role_match = match_roles(title_text, getattr(job, 'description', ''), profile)
    
    # Ensure extractor runs globally to grab `job.skills`
    extract_skills(job, profile)
    matched_skills = list(set(getattr(job, "skills", []) or []))
    
    # Keyword check
    preferred_keywords = get_profile_list(profile, "preferred_keywords")
    matched_keywords = [
        kw for kw in preferred_keywords
        if contains_term(job_text, kw)
    ]
    
    # Save cache struct
    job.match_data = {
        "role_match": role_match,
        "matched_skills": matched_skills,
        "matched_keywords": matched_keywords,
        "excluded": is_excluded
    }
