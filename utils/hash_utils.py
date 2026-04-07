import hashlib
from typing import Optional


def generate_job_hash(title: str, company: str, location: str) -> str:
    """
    Generate a SHA-256 hash for job deduplication based on title, company, and location.

    Args:
        title: Job title
        company: Company name
        location: Job location

    Returns:
        Hexadecimal hash string
    """
    content = f"{title.lower().strip()}{company.lower().strip()}{location.lower().strip()}"
    return hashlib.sha256(content.encode('utf-8')).hexdigest()