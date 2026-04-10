import re
from typing import Optional

def normalize_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    try:
        normalized = str(text).lower()
        normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized
    except Exception:
        return ""

def contains_term(text: Optional[str], term: Optional[str]) -> bool:
    normalized_text = normalize_text(text)
    normalized_term = normalize_text(term)
    if not normalized_text or not normalized_term:
        return False
    try:
        return re.search(rf"\b{re.escape(normalized_term)}\b", normalized_text) is not None
    except Exception:
        return False
