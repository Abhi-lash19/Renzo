from typing import Any, Dict

from intelligence.github_mapper import map_projects
from utils.logger import get_logger

logger = get_logger(__name__)

def generate_insight(job, profile: Dict[str, Any]) -> Dict[str, object]:
    try:
        matched_skills = getattr(job, "skills", [])
        missing_skills = getattr(job, "missing_skills", [])
        project_suggestions = map_projects(job, profile)

        if matched_skills:
            why_match = f"Strong match with {', '.join(matched_skills[:3])}"
        else:
            why_match = "Low match — consider improving skills"

        if missing_skills:
            recommendation = f"Missing {', '.join(missing_skills[:3])}"
        else:
            recommendation = "Resume already aligns well with this role"

        summary_suggestions = []
        if matched_skills:
            summary_suggestions.append(
                f"Lead with backend strengths in {', '.join(matched_skills[:3])} tied to outcomes."
            )
        if missing_skills:
            summary_suggestions.append(
                f"Do not claim {', '.join(missing_skills[:2])}; instead mention adjacent work honestly."
            )

        skill_highlights = [
            f"Highlight {skill} with a concrete production example."
            for skill in matched_skills[:4]
        ]

        return {
            "why_match": why_match,
            "missing_skills": missing_skills,
            "recommendation": recommendation,
            "summary_suggestions": summary_suggestions,
            "skill_highlights": skill_highlights,
            "project_suggestions": project_suggestions,
        }
    except Exception as e:
        logger.exception(f"Error generating insight: {e}")
        return {
            "why_match": "Low match — consider improving skills",
            "missing_skills": [],
            "recommendation": "",
            "summary_suggestions": [],
            "skill_highlights": [],
            "project_suggestions": [],
        }
