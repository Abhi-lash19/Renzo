from storage.repository import JobRepository
from utils.profile_loader import load_profile


def get_repository() -> JobRepository:
    return JobRepository()


def get_profile() -> dict:
    return load_profile()
