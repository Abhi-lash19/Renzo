import hashlib
from typing import Optional


def _normalize_value(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lower()


def generate_job_hash(
    title: Optional[str],
    company: Optional[str],
    location: Optional[str],
    source: Optional[str],
) -> str:
    """
    Generate a SHA-256 hash for job deduplication based on title, company, location, and source.

    Args:
        title: Job title
        company: Company name
        location: Job location
        source: Job source identifier

    Returns:
        Hexadecimal hash string
    """
    content = (
        _normalize_value(title)
        + _normalize_value(company)
        + _normalize_value(location)
        + _normalize_value(source)
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()