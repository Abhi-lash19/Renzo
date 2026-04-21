import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Set

if TYPE_CHECKING:
    from pipeline.models import Job

from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

logger = get_logger(__name__)

# Essential fallback configuration if profile misses them
DEFAULT_CONFIG = {
    "target_roles": ["developer", "backend", "python", "software engineer", "api", "microservices"],
    "exclude_keywords": ["frontend", "ui", "react", "angular", "mobile", "ios", "android", "flutter", "sales", "marketing", "senior", "lead", "principal"],
    "bonus_keywords": ["startup", "early stage", "remote", "cloud native", "saas"],
    "preferred_keywords": ["backend", "api", "microservices", "cloud", "aws", "rest", "lambda", "serverless"],
}

DEFAULT_SKILL_WEIGHT = 0.4
CORE_SKILL_WEIGHT = 1.0
SECONDARY_SKILL_WEIGHT = 0.6
PREFERRED_SKILL_WEIGHT = 0.5
PREFERRED_KEYWORD_WEIGHT = 1.0
BONUS_KEYWORD_WEIGHT = 0.6
BONUS_SCORE_MAX = 2.0
MAX_LEARNING_BOOST = 0.25
MAX_LEARNING_PENALTY = -0.25

SKILL_SOURCE_EXCLUDE_KEYS = {
    "target_roles",
    "preferred_roles",
    "exclude_keywords",
    "bonus_keywords",
    "preferred_keywords",
    "projects",
    "experience",
    "location",
    "name",
    "role",
    "experience_level",
    "remote_preferred",
    "is_empty",
}

# ---------------------------------------------------------------
# (A) SKILL NORMALIZATION LAYER
# ---------------------------------------------------------------
_NORMALIZATION_MAP = {
    "node js": "nodejs",
    "node.js": "nodejs",
    "node": "nodejs",
    "ci cd": "cicd",
    "ci/cd": "cicd",
    "api gateway": "apigateway",
    "amazon web services": "aws",
    "k8s": "kubernetes",
    "rest api": "api",
    "restful api": "api",
}


def normalize_skill(skill: str) -> str:
    """Normalize a skill string for deterministic matching and scoring."""
    if not skill:
        return ""
    normalized = skill.lower().strip()
    normalized = " ".join(normalized.split())
    return _NORMALIZATION_MAP.get(normalized, normalized)


# ---------------------------------------------------------------
# (B) SYNONYM MAP
# ---------------------------------------------------------------
SKILL_SYNONYMS = {
    "nodejs": ["node", "node.js", "node js"],
    "aws": ["amazon web services"],
    "kubernetes": ["k8s"],
    "cicd": ["ci/cd", "ci cd"],
    "api": ["rest api", "restful api"],
    "python": ["python3", "python 3"],
    "docker": ["containerization"],
    "sql": ["mysql", "postgresql", "postgres"],
    "terraform": ["iac", "infrastructure as code"],
}

_NEGATIVE_PATTERNS = [
    r"\bno(?:[\s\W]+){skill}\b",
    r"\bnot(?:[\s\W]+)required(?:[\s\W]+){skill}\b",
    r"\bwithout(?:[\s\W]+){skill}\b",
    r"\bdon[' ]?t(?:[\s\W]+)use(?:[\s\W]+){skill}\b",
    r"\bdo(?:[\s\W]+)not(?:[\s\W]+)use(?:[\s\W]+){skill}\b",
    r"\bexcluding(?:[\s\W]+){skill}\b",
    r"\b{skill}\b(?:[\s\W]+)not(?:[\s\W]+)required\b",
    r"\b{skill}\b(?:[\s\W]+)is(?:[\s\W]+)not(?:[\s\W]+)required\b",
    r"\b{skill}\b[\s\S]{{0,40}}\bnot(?:[\s\W]+)required\b",
]


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


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _dedupe_sorted(values: Iterable[str]) -> List[str]:
    return sorted({value for value in values if value})


def _iter_profile_skill_terms(profile: Dict[str, Any]) -> Iterable[str]:
    for key, value in profile.items():
        if key == "weighted_skills" or key in SKILL_SOURCE_EXCLUDE_KEYS:
            continue
        if isinstance(value, list):
            for item in value:
                if item:
                    yield item


def _extract_canonical_user_skills(profile: Dict[str, Any]) -> Set[str]:
    skills = {
        normalize_skill(skill)
        for skill in _iter_profile_skill_terms(profile)
        if normalize_skill(skill)
    }

    weighted_skills = profile.get("weighted_skills", {}) or {}
    if isinstance(weighted_skills, dict):
        for raw_skill in weighted_skills:
            canonical_skill = normalize_skill(raw_skill)
            if canonical_skill:
                skills.add(canonical_skill)

    return skills


def _build_profile_skill_map(profile: Dict[str, Any]) -> Dict[str, str]:
    skill_map: Dict[str, str] = {}
    for raw_skill in _iter_profile_skill_terms(profile):
        canonical_skill = normalize_skill(raw_skill)
        if canonical_skill:
            skill_map[normalize_text(raw_skill)] = canonical_skill
    return skill_map


def _expand_skill_terms(canonical_skill: str) -> List[str]:
    variants = {normalize_text(canonical_skill)}
    for synonym in SKILL_SYNONYMS.get(canonical_skill, []):
        normalized_synonym = normalize_text(synonym)
        if normalized_synonym:
            variants.add(normalized_synonym)
    return sorted(variants, key=lambda value: (-len(value), value))


def _build_weight_lookup(profile: Dict[str, Any], canonical_user_skills: Set[str]) -> Dict[str, float]:
    explicit_weights: Dict[str, float] = {}
    raw_weighted_skills = profile.get("weighted_skills", {}) or {}

    if isinstance(raw_weighted_skills, dict):
        for raw_skill, raw_weight in raw_weighted_skills.items():
            canonical_skill = normalize_skill(raw_skill)
            if canonical_skill:
                explicit_weights[canonical_skill] = _safe_float(raw_weight, DEFAULT_SKILL_WEIGHT)

    core_skills = {
        normalize_skill(skill)
        for skill in profile.get("core_skills", [])
        if normalize_skill(skill)
    }
    secondary_skills = {
        normalize_skill(skill)
        for skill in profile.get("secondary_skills", [])
        if normalize_skill(skill)
    }
    preferred_keyword_skills = {
        normalize_skill(skill)
        for skill in profile.get("preferred_keywords", [])
        if normalize_skill(skill)
    }

    weight_lookup: Dict[str, float] = {}
    for canonical_skill in canonical_user_skills:
        if canonical_skill in explicit_weights:
            weight_lookup[canonical_skill] = explicit_weights[canonical_skill]
        elif canonical_skill in core_skills:
            weight_lookup[canonical_skill] = CORE_SKILL_WEIGHT
        elif canonical_skill in secondary_skills:
            weight_lookup[canonical_skill] = SECONDARY_SKILL_WEIGHT
        elif canonical_skill in preferred_keyword_skills:
            weight_lookup[canonical_skill] = PREFERRED_SKILL_WEIGHT
        else:
            weight_lookup[canonical_skill] = DEFAULT_SKILL_WEIGHT

    return weight_lookup


def _term_to_regex(term: str) -> str:
    tokens = normalize_text(term).split()
    if not tokens:
        return ""
    return r"\b" + r"(?:[\s\W]+)".join(re.escape(token) for token in tokens) + r"\b"


def _has_negative_context(raw_text_lower: str, skill_term: str) -> bool:
    skill_pattern = _term_to_regex(skill_term)
    if not skill_pattern:
        return False

    for pattern in _NEGATIVE_PATTERNS:
        compiled = re.compile(pattern.format(skill=skill_pattern), re.IGNORECASE | re.DOTALL)
        if compiled.search(raw_text_lower):
            return True
    return False


def _evaluate_skill_match(
    canonical_skill: str,
    job_text_normalized: str,
    raw_job_text_lower: str,
) -> Dict[str, Any]:
    matched_aliases: List[str] = []
    saw_negative_context = False

    for candidate_term in _expand_skill_terms(canonical_skill):
        if not contains_term(job_text_normalized, candidate_term):
            continue
        if _has_negative_context(raw_job_text_lower, candidate_term):
            saw_negative_context = True
            continue
        matched_aliases.append(candidate_term)

    if matched_aliases:
        return {
            "matched": True,
            "reason": None,
            "aliases": _dedupe_sorted(matched_aliases),
        }

    return {
        "matched": False,
        "reason": "negative_context" if saw_negative_context else "no_match",
        "aliases": [],
    }


def get_profile_list(profile: Dict[str, Any], key: str) -> List[str]:
    return _unique_terms(profile.get(key, []), DEFAULT_CONFIG.get(key, []))


def has_any_term(text: str, terms: List[str]) -> bool:
    if not text or not terms:
        return False
    return any(term and contains_term(text, term) for term in terms)


def detect_exclusions(text: str, profile: Dict[str, Any]) -> bool:
    exclude_terms = get_profile_list(profile, "exclude_keywords")
    return has_any_term(text, exclude_terms)


def _build_keyword_matches(profile: Dict[str, Any], job_text_normalized: str) -> Dict[str, Any]:
    keyword_weights: Dict[str, float] = {}
    keyword_types: Dict[str, str] = {}

    for keyword in get_profile_list(profile, "preferred_keywords"):
        keyword_weights[keyword] = max(keyword_weights.get(keyword, 0.0), PREFERRED_KEYWORD_WEIGHT)
        keyword_types[keyword] = "preferred"

    for keyword in get_profile_list(profile, "bonus_keywords"):
        existing_weight = keyword_weights.get(keyword, 0.0)
        if BONUS_KEYWORD_WEIGHT > existing_weight:
            keyword_weights[keyword] = BONUS_KEYWORD_WEIGHT
            keyword_types[keyword] = "bonus"
        elif keyword not in keyword_types:
            keyword_types[keyword] = "bonus"

    matched_keywords = _dedupe_sorted(
        keyword for keyword in keyword_weights if contains_term(job_text_normalized, keyword)
    )
    matched_preferred_keywords = [
        keyword for keyword in matched_keywords if keyword_types.get(keyword) == "preferred"
    ]
    matched_bonus_keywords = [
        keyword for keyword in matched_keywords if keyword_types.get(keyword) == "bonus"
    ]

    keyword_score = sum(keyword_weights.get(keyword, 0.0) for keyword in matched_keywords)
    keyword_max_score = sum(keyword_weights.values())

    return {
        "matched_keywords": matched_keywords,
        "matched_preferred_keywords": matched_preferred_keywords,
        "matched_bonus_keywords": matched_bonus_keywords,
        "keyword_score": round(keyword_score, 4),
        "keyword_max_score": round(keyword_max_score, 4),
        "keyword_weights": {key: round(value, 4) for key, value in sorted(keyword_weights.items())},
    }


def _overlap_ratio(left: Set[str], right: Set[str]) -> float:
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / float(len(union))


def _build_learning_signals(
    job: "Job",
    profile: Dict[str, Any],
    matched_skills: List[str],
    matched_keywords: List[str],
) -> Dict[str, Any]:
    learned_preferences = profile.get("learned_preferences", {}) or {}
    if not learned_preferences:
        return {
            "learning_score": 0.0,
            "preferred_skill_hits": [],
            "ignored_skill_hits": [],
            "preferred_company_match": False,
            "preferred_role_match": False,
            "similarity_to_applied": 0.0,
            "learning_breakdown": {},
        }

    title_normalized = normalize_text(getattr(job, "title", ""))
    company_normalized = normalize_text(getattr(job, "company", ""))
    matched_skill_set = set(matched_skills)
    matched_keyword_set = set(matched_keywords)

    preferred_skill_hits = sorted(
        matched_skill_set & set(learned_preferences.get("preferred_skills", []))
    )
    ignored_skill_hits = sorted(
        matched_skill_set & set(learned_preferences.get("ignored_skills", []))
    )

    preferred_company_match = company_normalized in set(
        learned_preferences.get("preferred_companies", [])
    )
    preferred_role_match = any(
        contains_term(title_normalized, role)
        for role in learned_preferences.get("preferred_roles", [])
    )

    similarity_to_applied = 0.0
    for applied_profile in learned_preferences.get("applied_job_profiles", []):
        applied_skills = set(applied_profile.get("skills", []))
        applied_keywords = set(applied_profile.get("keywords", []))
        skill_overlap = _overlap_ratio(matched_skill_set, applied_skills)
        keyword_overlap = _overlap_ratio(matched_keyword_set, applied_keywords)
        similarity = round((skill_overlap * 0.7) + (keyword_overlap * 0.3), 4)
        if similarity > similarity_to_applied:
            similarity_to_applied = similarity

    preferred_skill_boost = min(len(preferred_skill_hits) * 0.1, 0.2)
    ignored_skill_penalty = min(len(ignored_skill_hits) * 0.1, 0.2)
    company_boost = 0.05 if preferred_company_match else 0.0
    role_boost = 0.05 if preferred_role_match else 0.0
    similarity_boost = min(similarity_to_applied * 0.08, 0.05)

    learning_score = preferred_skill_boost + company_boost + role_boost + similarity_boost - ignored_skill_penalty
    learning_score = round(min(max(learning_score, MAX_LEARNING_PENALTY), MAX_LEARNING_BOOST), 4)

    learning_breakdown = {
        "preferred_skill_boost": round(preferred_skill_boost, 4),
        "ignored_skill_penalty": round(ignored_skill_penalty, 4),
        "company_boost": round(company_boost, 4),
        "role_boost": round(role_boost, 4),
        "similarity_boost": round(similarity_boost, 4),
    }

    return {
        "learning_score": learning_score,
        "preferred_skill_hits": preferred_skill_hits,
        "ignored_skill_hits": ignored_skill_hits,
        "preferred_company_match": preferred_company_match,
        "preferred_role_match": preferred_role_match,
        "similarity_to_applied": round(similarity_to_applied, 4),
        "learning_breakdown": learning_breakdown,
    }


def match_roles(
    title: str,
    description: str,
    profile: Dict[str, Any],
    matched_preferred_keywords: List[str],
    matched_keywords: List[str],
) -> float:
    title_norm = normalize_text(title)
    description_norm = normalize_text(description)

    target_roles = get_profile_list(profile, "target_roles") or get_profile_list(profile, "preferred_roles")
    if not title_norm and not description_norm:
        return 0.0
    if not target_roles:
        return 0.0

    title_matches = [role for role in target_roles if contains_term(title_norm, role)]
    if title_matches:
        return 1.0

    description_matches = [role for role in target_roles if contains_term(description_norm, role)]
    if description_matches and matched_preferred_keywords:
        return 0.8

    if len(matched_keywords) >= 2:
        return 0.6

    if description_matches:
        return 0.4

    return 0.0


def _get_job_age_hours(job: "Job") -> float:
    try:
        posted_at = getattr(job, "posted_at", None)
        if not posted_at:
            return 0.0
        clean_posted_at = posted_at.replace(tzinfo=None) if posted_at.tzinfo else posted_at
        return max((datetime.utcnow() - clean_posted_at).total_seconds() / 3600.0, 0.0)
    except Exception:
        return 0.0


def _calculate_recency_score(job: "Job") -> float:
    age_hours = _get_job_age_hours(job)
    if age_hours < 24:
        return 1.0
    if age_hours < 72:
        return 0.7
    if age_hours < 168:
        return 0.4
    return 0.0


def _calculate_bonus_score(job: "Job") -> float:
    bonus_score = 0.0
    title = normalize_text(getattr(job, "title", ""))
    location = normalize_text(getattr(job, "location", ""))
    if contains_term(title, "remote") or contains_term(location, "remote") or getattr(job, "is_remote", False):
        bonus_score += 1.0
    if getattr(job, "is_startup", False):
        bonus_score += 1.0
    return bonus_score


def apply_match_data(job: "Job", match_data: Dict[str, Any]) -> None:
    """Project authoritative match_data fields onto the mutable job object."""
    job.match_data = match_data
    job.skills = list(match_data.get("matched_skills", []))
    job.detected_skills = list(match_data.get("matched_skills", []))
    job.missing_skills = list(match_data.get("missing_skills", []))


def build_match_data(job: "Job", profile: dict) -> dict:
    """
    Build standardized match_data for a job.

    match_data is the single source of truth for filtering, scoring, and insights.
    """
    if not job:
        raise ValueError("Job cannot be None")

    profile = profile or {}

    try:
        title = getattr(job, "title", "") or ""
        description = getattr(job, "description", "") or ""

        raw_job_text_lower = f"{title}\n{description}".lower()
        job_text_normalized = normalize_text(f"{title} {description}")
        title_normalized = normalize_text(title)

        canonical_user_skills = _extract_canonical_user_skills(profile)
        profile_skill_map = _build_profile_skill_map(profile)
        weight_lookup = _build_weight_lookup(profile, canonical_user_skills)

        matched_skills: Set[str] = set()
        rejected_skills: Dict[str, str] = {}
        matched_skill_aliases: Dict[str, List[str]] = {}

        for canonical_skill in sorted(canonical_user_skills):
            match_result = _evaluate_skill_match(canonical_skill, job_text_normalized, raw_job_text_lower)
            if match_result["matched"]:
                matched_skills.add(canonical_skill)
                matched_skill_aliases[canonical_skill] = match_result["aliases"]
            else:
                rejected_skills[canonical_skill] = match_result["reason"]

        matched_skills_list = sorted(matched_skills)
        missing_skills = sorted(canonical_user_skills - matched_skills)
        skill_score_raw = round(
            sum(weight_lookup.get(skill, DEFAULT_SKILL_WEIGHT) for skill in matched_skills_list),
            4,
        )
        skill_max_score = round(
            sum(weight_lookup.values()) if weight_lookup else (len(canonical_user_skills) * DEFAULT_SKILL_WEIGHT),
            4,
        )
        skill_overlap = len(matched_skills_list)

        keyword_match_data = _build_keyword_matches(profile, job_text_normalized)
        role_match_score = match_roles(
            title=title,
            description=description,
            profile=profile,
            matched_preferred_keywords=keyword_match_data["matched_preferred_keywords"],
            matched_keywords=keyword_match_data["matched_keywords"],
        )
        excluded = detect_exclusions(job_text_normalized, profile)

        title_match = 1.0 if any(
            contains_term(title_normalized, role)
            for role in get_profile_list(profile, "target_roles")
        ) else 0.0
        experience_match = 1.0
        recency_score = _calculate_recency_score(job)
        bonus_score = _calculate_bonus_score(job)
        focus_boost = len(keyword_match_data["matched_bonus_keywords"])

        weight_assignment = {
            skill: round(weight_lookup.get(skill, DEFAULT_SKILL_WEIGHT), 4)
            for skill in sorted(canonical_user_skills)
        }

        learning_signals = _build_learning_signals(
            job=job,
            profile=profile,
            matched_skills=matched_skills_list,
            matched_keywords=keyword_match_data["matched_keywords"],
        )

        match_data = {
            "matched_skills": matched_skills_list,
            "missing_skills": missing_skills,
            "skill_score_raw": skill_score_raw,
            "skill_overlap": skill_overlap,
            "role_match_score": round(role_match_score, 4),
            "excluded": excluded,
            "matched_keywords": keyword_match_data["matched_keywords"],
            "keyword_score": keyword_match_data["keyword_score"],
            "role_match": role_match_score > 0.0,
            "title_match": title_match,
            "experience_match": experience_match,
            "normalized_skills": sorted(canonical_user_skills),
            "rejected_skills": rejected_skills,
            "weight_assignment": weight_assignment,
            "skill_alias_matches": matched_skill_aliases,
            "profile_skill_map": profile_skill_map,
            "skill_max_score": skill_max_score,
            "matched_preferred_keywords": keyword_match_data["matched_preferred_keywords"],
            "matched_bonus_keywords": keyword_match_data["matched_bonus_keywords"],
            "keyword_max_score": keyword_match_data["keyword_max_score"],
            "keyword_weights": keyword_match_data["keyword_weights"],
            "recency_score": recency_score,
            "bonus_score": bonus_score,
            "bonus_max_score": BONUS_SCORE_MAX,
            "focus_boost": focus_boost,
            "learning_score": learning_signals["learning_score"],
            "preferred_skill_hits": learning_signals["preferred_skill_hits"],
            "ignored_skill_hits": learning_signals["ignored_skill_hits"],
            "preferred_company_match": learning_signals["preferred_company_match"],
            "preferred_role_match": learning_signals["preferred_role_match"],
            "similarity_to_applied": learning_signals["similarity_to_applied"],
            "learning_breakdown": learning_signals["learning_breakdown"],
        }

        apply_match_data(job, match_data)

        logger.info(
            f"[MATCH_SKILLS] job_id={getattr(job, 'job_id', 'unknown')} "
            f"matched_skills={match_data['matched_skills']} "
            f"rejected_skills={match_data['rejected_skills']} "
            f"weight_assignment={match_data['weight_assignment']} "
            f"skill_alias_matches={match_data['skill_alias_matches']}"
        )
        logger.info(
            f"[MATCH_SCORE_INPUT] job_id={getattr(job, 'job_id', 'unknown')} "
            f"skill_score_raw={match_data['skill_score_raw']} "
            f"skill_max_score={match_data['skill_max_score']} "
            f"keyword_score={match_data['keyword_score']} "
            f"keyword_max_score={match_data['keyword_max_score']} "
            f"role_match_score={match_data['role_match_score']} "
            f"excluded={match_data['excluded']}"
        )
        logger.info(
            f"[LEARNING_MATCH] job_id={getattr(job, 'job_id', 'unknown')} "
            f"learning_score={match_data['learning_score']} "
            f"preferred_skill_hits={match_data['preferred_skill_hits']} "
            f"ignored_skill_hits={match_data['ignored_skill_hits']} "
            f"similarity_to_applied={match_data['similarity_to_applied']} "
            f"learning_breakdown={match_data['learning_breakdown']}"
        )

        return match_data

    except Exception as error:
        logger.exception(
            f"[MATCH_BUILD_ERROR] job_id={getattr(job, 'job_id', 'unknown')} error={error}"
        )
        raise
