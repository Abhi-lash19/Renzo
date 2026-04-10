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
    source: Optional[str] = None,
) -> str:
    """
    Generate a SHA-256 hash for job deduplication based on title, company, and location.

    Args:
        title: Job title
        company: Company name
        location: Job location
    Returns:
        Hexadecimal hash string
    """
    content = (
        _normalize_value(title)
        + _normalize_value(company)
        + _normalize_value(location)
    )
    return hashlib.sha256(content.encode("utf-8")).hexdigest()
