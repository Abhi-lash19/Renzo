"""
resume/builder.py — Build a pipeline-compatible profile dict from a ParsedResume.

Output is structurally identical to utils/profile_loader.load_profile() so it can
be passed directly to build_match_data(), score_job(), etc. without modification.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List

from resume.normalizer import categorize_skills, normalize_skills, CORE_BACKEND_SIGNALS
from utils.logger import get_logger
from utils.text_utils import normalize_text

logger = get_logger(__name__)

_DEFAULT_EXCLUDES = [
    "frontend", "ui", "react", "angular", "mobile", "ios", "android",
    "flutter", "wordpress", "php", "shopify",
]
_DEFAULT_BONUS = ["startup", "early stage", "remote", "saas", "cloud native"]
_DEFAULT_PREFERRED_KEYWORDS = [
    "backend", "api", "microservices", "cloud", "aws", "rest", "serverless",
]

_EXPERIENCE_LEVEL_RE = re.compile(
    r"(\d+)\s*[\+\-–]?\s*(\d*)\s*(?:year|yr)", re.IGNORECASE
)


def _infer_experience_level(text: str) -> str:
    """Heuristically infer years-of-experience category."""
    matches = _EXPERIENCE_LEVEL_RE.findall(text or "")
    if not matches:
        return "0-2 years"
    years = [int(m[0]) for m in matches if m[0].isdigit()]
    if not years:
        return "0-2 years"
    max_years = max(years)
    if max_years >= 5:
        return "5+ years"
    if max_years >= 2:
        return "2-5 years"
    return "0-2 years"


def _dedupe(items: List[str]) -> List[str]:
    return list(dict.fromkeys(item for item in items if item))


def _build_weighted_skills(
    core: List[str],
    secondary: List[str],
    preferred: List[str],
) -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for skill in core:
        weights[skill] = 1.0
    for skill in secondary:
        weights.setdefault(skill, 0.6)
    for kw in preferred:
        normalized_kw = normalize_text(kw) if kw else kw
        if normalized_kw:
            weights.setdefault(normalized_kw, 0.5)
    return {k: v for k, v in weights.items() if k}


def build_profile(parsed_resume) -> Dict[str, Any]:
    """
    Build a pipeline-compatible profile dict from a ParsedResume instance.

    Args:
        parsed_resume: ParsedResume dataclass from resume/parser.py.

    Returns:
        Full profile dict ready for the matching pipeline.
    """
    normalized = normalize_skills(parsed_resume.raw_skills)
    categories = categorize_skills(normalized)
    core_skills: List[str] = categories["core_skills"]
    secondary_skills: List[str] = categories["secondary_skills"]

    # Also mine experience text for core signals
    experience_text = " ".join(parsed_resume.experience_items)
    extra_core = [
        s for s in CORE_BACKEND_SIGNALS
        if re.search(r"\b" + re.escape(s) + r"\b", experience_text, re.IGNORECASE)
        and s not in core_skills
        and s not in secondary_skills
    ]
    core_skills = _dedupe(core_skills + extra_core)
    all_skills = _dedupe(core_skills + secondary_skills)

    # Roles
    preferred_roles = _dedupe(parsed_resume.detected_roles) or [
        "backend developer", "software engineer"
    ]
    target_roles = _dedupe(parsed_resume.detected_roles) or ["backend", "python", "api"]

    # Experience level from combined text
    all_text = " ".join([
        parsed_resume.summary,
        " ".join(parsed_resume.experience_items),
        parsed_resume.extra_text,
    ])
    experience_level = _infer_experience_level(all_text)

    weighted_skills = _build_weighted_skills(
        core_skills, secondary_skills, _DEFAULT_PREFERRED_KEYWORDS
    )
    if not weighted_skills and all_skills:
        weighted_skills = {skill: 0.7 for skill in all_skills}

    profile: Dict[str, Any] = {
        "name": parsed_resume.name or "",
        "role": preferred_roles[0] if preferred_roles else "",
        "experience_level": experience_level,
        "core_skills": core_skills,
        "secondary_skills": secondary_skills,
        "all_skills": all_skills,
        "weighted_skills": weighted_skills,
        "cloud": [s for s in core_skills if any(
            kw in s for kw in ["aws", "gcp", "azure", "cloud", "s3", "sqs", "lambda", "ec2"]
        )],
        "devops": [s for s in (core_skills + secondary_skills) if any(
            kw in s for kw in ["docker", "kubernetes", "terraform", "ansible", "cicd", "jenkins"]
        )],
        "projects": _dedupe(parsed_resume.project_items),
        "experience": _dedupe(parsed_resume.experience_items),
        "preferred_roles": preferred_roles,
        "target_roles": target_roles,
        "exclude_keywords": _DEFAULT_EXCLUDES,
        "bonus_keywords": _DEFAULT_BONUS,
        "preferred_keywords": _DEFAULT_PREFERRED_KEYWORDS,
        "location": "",
        "remote_preferred": True,
        "is_empty": len(all_skills) == 0,
        "source": "upload",
    }

    logger.info(
        f"[BUILDER] name='{profile['name']}' core={len(core_skills)} "
        f"secondary={len(secondary_skills)} all={len(all_skills)}"
    )
    return profile


def recompute_profile_skills(profile: dict) -> dict:
    """
    Recompute all_skills and weighted_skills after a manual profile update.

    Call this whenever core_skills or secondary_skills change outside of the
    normal build_profile() flow (e.g. after a PUT /profile merge-patch).

    Modifies `profile` in-place and also returns it.
    """
    combined = (
        profile.get("core_skills", [])
        + profile.get("secondary_skills", [])
        + profile.get("cloud", [])
        + profile.get("devops", [])
    )
    profile["all_skills"] = _dedupe(normalize_skills(combined))
    profile["weighted_skills"] = _build_weighted_skills(
        profile.get("core_skills", []),
        profile.get("secondary_skills", []),
        profile.get("preferred_keywords", []),
    )
    return profile
