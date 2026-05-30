import json as _json

from fastapi import Depends as _Depends

from storage.repository import JobRepository
from utils.profile_loader import load_profile
from app.auth import get_current_user  # noqa: F401 — re-exported for convenience


def get_repository() -> JobRepository:
    return JobRepository()


def get_profile() -> dict:
    return load_profile()


def get_user_profile(
    current_user: dict = _Depends(get_current_user),
    repository: JobRepository = _Depends(get_repository),
) -> dict:
    """
    Load the user's profile from DB (if uploaded) or fall back to config/profile.txt.
    Use in endpoints that need per-user profiles (e.g. run-job-finder in future phases).
    """
    user_id = str(current_user.get("sub") or current_user.get("id", "anonymous"))
    stored = repository.get_profile_by_user(user_id)
    if stored and stored.get("profile_json"):
        try:
            profile = _json.loads(stored["profile_json"])
            profile["source"] = stored.get("source", "upload")
            return profile
        except Exception:
            pass
    return load_profile()
