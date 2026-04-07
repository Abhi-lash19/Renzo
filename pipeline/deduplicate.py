from typing import TYPE_CHECKING
from utils.hash_utils import generate_job_hash
from storage.repository import JobRepository
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


def is_duplicate(job: 'Job', repository: JobRepository) -> bool:
    """
    Check if the job is a duplicate based on its hash.

    Args:
        job: Job instance
        repository: JobRepository instance

    Returns:
        True if duplicate, False otherwise
    """
    job_hash = generate_job_hash(job.title, job.company, job.location)

    # Try inserting directly
    success = repository.insert_hash(job_hash)

    if not success:
        logger.debug(f"Duplicate job detected: {job.title} at {job.company}")
        return True

    # Insert the hash if not exists
    repository.insert_hash(job_hash)
    return False