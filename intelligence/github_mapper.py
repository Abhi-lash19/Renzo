from typing import Any, Dict, List

from utils.logger import get_logger

logger = get_logger(__name__)


def map_projects(job, profile: Dict[str, Any]) -> List[str]:
    try:
        suggestions: List[str] = []
        matched_skills = getattr(job, "skills", [])
        missing_skills = getattr(job, "missing_skills", [])
        profile_projects = profile.get("projects", [])

        for skill in matched_skills[:3]:
            suggestions.append(f"Show a project highlighting {skill} with measurable backend impact.")
        for skill in missing_skills[:2]:
            suggestions.append(f"Build or refine a GitHub project that demonstrates {skill}.")
        for project in profile_projects[:2]:
            suggestions.append(f"Surface your {project} work if it includes production-like tradeoffs.")

        return list(dict.fromkeys(suggestions))
    except Exception as e:
        logger.exception(f"Error mapping GitHub projects: {e}")
        return []
