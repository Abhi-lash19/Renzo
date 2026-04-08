from pathlib import Path
from typing import Dict, List

DEFAULT_KEYS = [
    "core_skills",
    "secondary_skills",
    "preferred_roles",
    "exclude_keywords",
    "bonus_keywords",
    "preferred_keywords",
]


def _parse_value(value: str) -> List[str]:
    return [item.strip().lower() for item in value.split(",") if item.strip()]


def load_profile(file_path: str = "config/profile.txt") -> Dict[str, List[str]]:
    """Load a user profile from a simple key/value text file."""
    profile = {key: [] for key in DEFAULT_KEYS}
    path = Path(file_path)

    if not path.exists():
        return profile

    with path.open("r", encoding="utf-8") as profile_file:
        for raw_line in profile_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if ":" not in line:
                continue

            key, value = line.split(":", 1)
            normalized_key = key.strip().lower()
            if normalized_key not in profile:
                continue

            profile[normalized_key].extend(_parse_value(value))

    return profile
