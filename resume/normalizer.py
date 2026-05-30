"""
resume/normalizer.py — Canonicalize and categorize skills extracted from a resume.

Uses normalize_skill() from utils/matching_engine for consistent normalization
across the matching pipeline.
"""
from __future__ import annotations

from typing import Dict, List

from utils.matching_engine import normalize_skill
from utils.logger import get_logger

logger = get_logger(__name__)

CORE_BACKEND_SIGNALS: set[str] = {
    "python", "aws", "backend", "api", "rest", "restful", "microservices",
    "fastapi", "django", "flask", "starlette", "postgresql", "postgres",
    "mysql", "mongodb", "redis", "elasticsearch", "docker", "kubernetes",
    "lambda", "serverless", "cloud", "golang", "go", "rust", "java",
    "nodejs", "typescript", "kafka", "rabbitmq", "celery", "grpc",
    "terraform", "ansible", "cicd", "github actions", "gitlab ci",
    "sql", "nosql", "dynamodb", "s3", "sqs", "sns", "ec2",
    "spring", "spring boot", "express", "nestjs", "gin",
}

_MIN_SKILL_LEN = 2
_MAX_SKILL_LEN = 80


def normalize_skills(raw_skills: List[str]) -> List[str]:
    """
    Normalize a list of raw skill strings into canonical, deduplicated form.

    Steps:
    1. Strip whitespace
    2. Apply normalize_skill() (lowercases, maps synonyms)
    3. Filter empty and implausibly short/long entries
    4. Deduplicate preserving first-seen order

    Args:
        raw_skills: Raw skill strings from parser output.

    Returns:
        Ordered, deduplicated list of canonical skill strings.
    """
    seen: dict[str, bool] = {}
    result: List[str] = []

    for raw in raw_skills:
        stripped = (raw or "").strip()
        if not stripped:
            continue

        canonical = normalize_skill(stripped)
        if not canonical:
            continue

        if len(canonical) < _MIN_SKILL_LEN or len(canonical) > _MAX_SKILL_LEN:
            continue

        if canonical not in seen:
            seen[canonical] = True
            result.append(canonical)

    logger.debug(f"[NORMALIZER] {len(raw_skills)} raw → {len(result)} canonical skills")
    return result


def categorize_skills(skills: List[str]) -> Dict[str, List[str]]:
    """
    Classify a normalized skill list into core and secondary buckets.

    Core skills match any entry in CORE_BACKEND_SIGNALS (exact or substring for signals >= 4 chars).
    Secondary skills are everything else.

    Args:
        skills: List of normalized (canonical) skill strings.

    Returns:
        Dict with keys "core_skills" and "secondary_skills".
    """
    core: List[str] = []
    secondary: List[str] = []

    for skill in skills:
        is_core = (
            skill in CORE_BACKEND_SIGNALS
            or any(signal in skill for signal in CORE_BACKEND_SIGNALS if len(signal) >= 4)
        )
        if is_core:
            core.append(skill)
        else:
            secondary.append(skill)

    logger.debug(f"[NORMALIZER] core={len(core)} secondary={len(secondary)}")
    return {"core_skills": core, "secondary_skills": secondary}
