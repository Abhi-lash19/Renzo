from typing import Any, Dict

from utils.logger import get_logger
from utils.text_utils import contains_term, normalize_text

logger = get_logger(__name__)

BASE_SKILL_KEYWORDS = {
    "python",
    "django",
    "flask",
    "fastapi",
    "api",
    "rest",
    "graphql",
    "sql",
    "mysql",
    "postgresql",
    "mongodb",
    "redis",
    "docker",
    "kubernetes",
    "aws",
    "lambda",
    "ec2",
    "s3",
    "sqs",
    "sns",
    "api gateway",
    "ecs",
    "eks",
    "microservices",
    "backend",
    "automation",
    "terraform",
    "github actions",
    "gitlab ci",
    "node js",
    "datadog",
    "mulesoft",
    "distributed systems",
    "system design",
    "event driven architecture",
    "ci cd",
    "cloud",
    "saas",
    "analytics",
}


def _normalize_skill(skill: str) -> str:
    normalized = normalize_text(skill)
    aliases = {
        "node js": "node.js",
        "ci cd": "ci/cd",
    }
    return aliases.get(normalized, normalized)


def extract_skills(job, profile: Dict[str, Any]) -> None:
    try:
        if getattr(job, "detected_skills", None):
            return

        raw_text = " ".join(filter(None, [job.title, job.description]))
        normalized_text = normalize_text(raw_text)

        if not normalized_text:
            job.detected_skills = []
            job.skills = []
            return

        master_keywords = {
            _normalize_skill(skill)
            for skill in (
                profile.get("all_skills", [])
                + profile.get("preferred_keywords", [])
                + profile.get("bonus_keywords", [])
                + list(BASE_SKILL_KEYWORDS)
            )
            if skill
        }

        detected = {skill for skill in master_keywords if contains_term(normalized_text, skill)}
        profile_skills = {_normalize_skill(skill) for skill in profile.get("all_skills", [])}

        job.detected_skills = sorted(detected)
        job.skills = sorted(detected & profile_skills)
    except Exception as e:
        logger.exception(f"Error extracting skills: {e}")
        job.detected_skills = []
        job.skills = []
