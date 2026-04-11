import hashlib
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from pipeline.models import Job


def _normalize_value(value: Optional[str]) -> str:
    if not value:
        return ""
    return value.strip().lower()


def _normalize_url(url: Optional[str]) -> str:
    normalized = _normalize_value(url)
    if not normalized:
        return ""
    parts = urlsplit(normalized)
    normalized_path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, normalized_path, parts.query, ""))


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def generate_job_hash(
    title: Optional[str],
    company: Optional[str],
    location: Optional[str] = None,
    source: Optional[str] = None,
) -> str:
    """
    Generate a SHA-256 hash for fallback deduplication based on stable job fields.

    Args:
        title: Job title
        company: Company name
        location: Job location
        source: Job source
    Returns:
        Hexadecimal hash string
    """
    content = "|".join(
        [
            _normalize_value(title),
            _normalize_value(company),
            _normalize_value(location),
            _normalize_value(source),
        ]
    )
    return _hash_text(content)


def build_job_identity(job: "Job") -> tuple[str, str]:
    """
    Build the preferred deduplication identity for a job.

    Priority:
    1. URL
    2. job_id
    3. fallback content hash
    """
    normalized_url = _normalize_url(getattr(job, "url", None))
    if normalized_url:
        return "url", _hash_text(f"url|{normalized_url}")

    normalized_job_id = _normalize_value(getattr(job, "job_id", None))
    if normalized_job_id:
        return "job_id", _hash_text(f"job_id|{normalized_job_id}")

    return (
        "hash",
        generate_job_hash(
            getattr(job, "title", None),
            getattr(job, "company", None),
            getattr(job, "location", None),
            getattr(job, "source", None),
        ),
    )
