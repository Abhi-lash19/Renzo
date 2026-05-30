from storage.repository import JobRepository
from utils.profile_loader import load_profile
from app.auth import get_current_user  # noqa: F401 — re-exported for convenience


def get_repository() -> JobRepository:
    return JobRepository()


def get_profile() -> dict:
    return load_profile()
