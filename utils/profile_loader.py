from pathlib import Path
from typing import Any, Dict, List

from utils.logger import get_logger
from utils.text_utils import normalize_text
from utils.matching_engine import DEFAULT_CONFIG, normalize_skill

logger = get_logger(__name__)

DEFAULT_KEYS = [
    "core_skills",
    "secondary_skills",
    "cloud",
    "devops",
    "preferred_roles",
    "exclude_keywords",
    "bonus_keywords",
    "preferred_keywords",
    "projects",
    "experience",
    "target_roles"
]

FALLBACK_PROFILE = {
    "core_skills": ["python", "backend", "api", "aws", "sql"],
    "secondary_skills": ["docker", "fastapi", "microservices", "rest", "cloud"],
    "projects": ["api platform", "automation", "microservices", "analytics"],
    "experience": ["backend development", "cloud automation", "system integration"],
    **DEFAULT_CONFIG
}

def _parse_value(value: str) -> List[str]:
    return [normalize_text(item) for item in value.split(",") if normalize_text(item)]


def _dedupe(items: List[str]) -> List[str]:
    return list(dict.fromkeys(item for item in items if item))


def _canonicalize_weighted_skills(weights: Dict[str, float]) -> Dict[str, float]:
    canonical_weights: Dict[str, float] = {}
    for raw_skill, raw_weight in (weights or {}).items():
        canonical_skill = normalize_skill(raw_skill)
        if not canonical_skill:
            continue
        try:
            weight = float(raw_weight)
        except (TypeError, ValueError):
            continue
        canonical_weights[canonical_skill] = max(canonical_weights.get(canonical_skill, 0.0), weight)
    return canonical_weights


def _apply_defaults(profile: Dict[str, Any]) -> Dict[str, Any]:
    for key in DEFAULT_KEYS:
        profile[key] = _dedupe(profile.get(key, []) + FALLBACK_PROFILE.get(key, []))
    return profile

def load_profile(file_path: str = "config/profile.txt") -> Dict[str, Any]:
    profile = {key: [] for key in DEFAULT_KEYS}
    path = Path(file_path)

    try:
        if path.exists():
            current_key = None
            with path.open("r", encoding="utf-8") as profile_file:
                for raw_line in profile_file:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                        
                    if ":" in line:
                        key, value = line.split(":", 1)
                        normalized_key = key.strip().lower()
                        if normalized_key in profile or normalized_key == "target_roles":
                            current_key = normalized_key
                            if current_key not in profile:
                                profile[current_key] = []
                            if value.strip():
                                profile[current_key].extend(_parse_value(value))
                    elif current_key:
                        profile[current_key].extend(_parse_value(line))
                        
    except Exception as e:
        logger.exception(f"Error loading profile: {e}")

    profile = _apply_defaults(profile)
    # Build all_skills from core + secondary + cloud + devops
    combined = (
        profile["core_skills"]
        + profile["secondary_skills"]
        + profile.get("cloud", [])
        + profile.get("devops", [])
    )
    # Normalize all skills for consistent matching
    profile["all_skills"] = _dedupe([normalize_skill(s) for s in combined if s])

    weights: Dict[str, float] = {}
    for skill in profile["core_skills"]:
        weights[normalize_skill(skill)] = 1.0
    for skill in profile["secondary_skills"]:
        weights.setdefault(normalize_skill(skill), 0.6)
    for keyword in profile["preferred_keywords"]:
        weights.setdefault(normalize_skill(keyword), 0.5)
    profile["weighted_skills"] = _canonicalize_weighted_skills(weights)

    total_items = sum(len(items) for items in profile.values() if isinstance(items, list))
    if total_items == 0:
        logger.warning("Profile is empty; forcing fallback profile")
        profile["is_empty"] = True
        profile = _apply_defaults({key: [] for key in DEFAULT_KEYS})
        combined_fb = (
            profile["core_skills"]
            + profile["secondary_skills"]
            + profile.get("cloud", [])
            + profile.get("devops", [])
        )
        profile["all_skills"] = _dedupe([normalize_skill(s) for s in combined_fb if s])
        profile["weighted_skills"] = _canonicalize_weighted_skills({
            **{skill: 1.0 for skill in profile["core_skills"]},
            **{skill: 0.6 for skill in profile["secondary_skills"]},
        })
    else:
        profile["is_empty"] = False

    if not profile["weighted_skills"]:
        profile["weighted_skills"] = _canonicalize_weighted_skills({
            skill: 0.7 for skill in profile["all_skills"]
        })

    return profile
