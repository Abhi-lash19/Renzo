"""
Input validation for job postings and user profiles.

Functions raise RenzoValidationError on the first invalid field found so
callers can decide whether to skip the record or abort the pipeline stage.
"""

from core.exceptions import RenzoValidationError


def validate_job(job) -> None:
    """Raise RenzoValidationError if the job is missing required fields."""
    job_id = getattr(job, "job_id", "unknown")
    if not getattr(job, "url", None):
        raise RenzoValidationError(f"Job {job_id} has no URL")
    if not getattr(job, "title", None):
        raise RenzoValidationError(f"Job {job_id} has no title")
    if not getattr(job, "description", None):
        raise RenzoValidationError(f"Job {job_id} has no description")


def validate_profile(profile: dict) -> None:
    """Raise RenzoValidationError if the profile is missing required keys."""
    if not profile.get("core_skills"):
        raise RenzoValidationError("Profile must have at least one core skill")
    if not profile.get("weighted_skills"):
        raise RenzoValidationError("Profile must have weighted_skills populated")
