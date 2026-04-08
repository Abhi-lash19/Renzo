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

    if repository.hash_exists(job_hash):
        logger.info(f"Duplicate skipped: {job.title}")
        return True

    if not repository.insert_hash(job_hash):
        logger.warning(f"Unable to record hash for {job.title}. Proceeding without duplicate protection.")
        return False

    return False