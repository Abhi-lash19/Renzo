from collections import Counter
from typing import Any, Dict, List

from storage.repository import JobRepository
from utils.logger import get_logger
from utils.matching_engine import get_profile_list
from utils.text_utils import contains_term, normalize_text

logger = get_logger(__name__)

VALID_ACTIONS = {"viewed", "applied", "ignored"}


def _extract_role_signature(title: str, profile: Dict[str, Any]) -> str:
    normalized_title = normalize_text(title)
    if not normalized_title:
        return ""

    candidate_roles = get_profile_list(profile, "target_roles") + get_profile_list(profile, "preferred_roles")
    for role in candidate_roles:
        if contains_term(normalized_title, role):
            return role
    return normalized_title


def _extract_profile_keywords(title: str, description: str, profile: Dict[str, Any]) -> List[str]:
    searchable_text = normalize_text(f"{title} {description}")
    candidate_keywords = list(dict.fromkeys(
        get_profile_list(profile, "preferred_keywords") + get_profile_list(profile, "bonus_keywords")
    ))
    return sorted([keyword for keyword in candidate_keywords if contains_term(searchable_text, keyword)])


def record_interaction(repository: JobRepository, job_id: str, action: str) -> bool:
    return repository.record_interaction(job_id, action)


def get_user_preferences(repository: JobRepository, profile: Dict[str, Any], limit: int = 200) -> Dict[str, Any]:
    interaction_jobs = repository.get_interaction_jobs(limit=limit)
    applied_jobs = [item for item in interaction_jobs if item.get("action") == "applied"]
    ignored_jobs = [item for item in interaction_jobs if item.get("action") == "ignored"]
    viewed_jobs = [item for item in interaction_jobs if item.get("action") == "viewed"]

    preferred_skill_counter: Counter[str] = Counter()
    ignored_skill_counter: Counter[str] = Counter()
    preferred_company_counter: Counter[str] = Counter()
    preferred_role_counter: Counter[str] = Counter()
    applied_job_profiles: List[Dict[str, Any]] = []

    for job in applied_jobs:
        skills = sorted(set(job.get("skills", []) or []))
        keywords = _extract_profile_keywords(job.get("title", ""), job.get("description", ""), profile)
        company = normalize_text(job.get("company", ""))
        role = _extract_role_signature(job.get("title", ""), profile)

        preferred_skill_counter.update(skills)
        if company:
            preferred_company_counter.update([company])
        if role:
            preferred_role_counter.update([role])

        applied_job_profiles.append({
            "job_id": job.get("job_id"),
            "skills": skills,
            "keywords": keywords,
            "company": company,
            "role": role,
        })

    for job in ignored_jobs:
        ignored_skill_counter.update(sorted(set(job.get("skills", []) or [])))

    preferences = {
        "viewed_jobs_count": len(viewed_jobs),
        "applied_jobs_count": len(applied_jobs),
        "ignored_jobs_count": len(ignored_jobs),
        "preferred_skills": [skill for skill, count in preferred_skill_counter.most_common(10) if count >= 1],
        "ignored_skills": [skill for skill, count in ignored_skill_counter.most_common(10) if count >= 1],
        "preferred_companies": [company for company, _ in preferred_company_counter.most_common(10)],
        "preferred_roles": [role for role, _ in preferred_role_counter.most_common(10)],
        "applied_job_profiles": applied_job_profiles[:25],
        "last_actions": [
            {
                "job_id": item.get("job_id"),
                "action": item.get("action"),
                "created_at": item.get("created_at"),
            }
            for item in interaction_jobs[:10]
        ],
    }

    logger.info(
        f"[PREFERENCE_UPDATE] applied={preferences['applied_jobs_count']} "
        f"ignored={preferences['ignored_jobs_count']} viewed={preferences['viewed_jobs_count']} "
        f"preferred_skills={preferences['preferred_skills'][:5]} "
        f"ignored_skills={preferences['ignored_skills'][:5]} "
        f"preferred_roles={preferences['preferred_roles'][:5]}"
    )
    return preferences


def attach_user_preferences(profile: Dict[str, Any], preferences: Dict[str, Any]) -> Dict[str, Any]:
    profile["learned_preferences"] = preferences or {}
    return profile
