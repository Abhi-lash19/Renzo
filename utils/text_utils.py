import re
from typing import Optional


def normalize_text(text: Optional[str]) -> str:
    """Normalize text for safe token matching and comparison."""
    if not text:
        return ""

    normalized = text.lower()
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def contains_term(text: Optional[str], term: Optional[str]) -> bool:
    """Check if a normalized term exists as a full token or phrase in text."""
    normalized_text = normalize_text(text)
    normalized_term = normalize_text(term)
    if not normalized_text or not normalized_term:
        return False

    return re.search(rf"\b{re.escape(normalized_term)}\b", normalized_text) is not None
