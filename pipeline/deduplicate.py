from typing import TYPE_CHECKING
from utils.hash_utils import generate_job_hash
from storage.repository import JobRepository
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

_seen_hashes = []


def is_duplicate(job: 'Job', repository: JobRepository) -> bool:
    """
    Check if the job is a duplicate based on its hash.

    Args:
        job: Job instance
        repository: JobRepository instance

    Returns:
        True if duplicate, False otherwise
    """
    if not job.title or not job.company or not job.location:
        logger.debug(
            f"Invalid dedup input: job_id={getattr(job, 'job_id', 'unknown')} "
            f"title={job.title or 'missing'} company={job.company or 'missing'} "
            f"location={job.location or 'missing'}"
        )
        return False

    job_hash = generate_job_hash(job.title, job.company, job.location, job.source)
    logger.debug(
        f"Dedup hash: job_id={getattr(job, 'job_id', 'unknown')} "
        f"hash={job_hash} title={job.title or 'Unknown'} source={job.source or 'Unknown'}"
    )

    if len(_seen_hashes) < 5:
        _seen_hashes.append(job_hash)
        logger.debug(f"Sample dedup hash [{len(_seen_hashes)}]: {job_hash}")

    if repository.hash_exists(job_hash):
        logger.debug(
            f"Duplicate skipped: job_id={getattr(job, 'job_id', 'unknown')} "
            f"title={job.title or 'Unknown'} source={job.source or 'Unknown'}"
        )
        return True

    if not repository.insert_hash(job_hash):
        logger.warning(
            f"Unable to record hash for job_id={getattr(job, 'job_id', 'unknown')} "
            f"title={job.title or 'Unknown'} source={job.source or 'Unknown'}"
        )
        return False

    return False