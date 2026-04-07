from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.remotive_api import RemotiveFetcher
from pipeline.models import Job
from pipeline.filter import passes_filter
from pipeline.deduplicate import is_duplicate
from storage.repository import JobRepository
from utils.logger import get_logger
from config.settings import settings
from storage.db import init_db

logger = get_logger(__name__)


def fetch_all_jobs() -> List[Job]:
    """Fetch jobs from all sources concurrently with timing and error handling."""
    sources = [
        IndeedRSSFetcher(),
        AdzunaFetcher(),
        RemotiveFetcher()
    ]

    all_jobs = []

    with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
        future_to_source = {
            executor.submit(source.fetch_and_normalize): source.__class__.__name__
            for source in sources
        }

        for future in as_completed(future_to_source):
            source_name = future_to_source[future]

            try:
                start_time = time.time()
                jobs = future.result()
                execution_time = time.time() - start_time
                logger.info(f"✅ {source_name}: {len(jobs)} jobs fetched in {execution_time:.2f}s")
                all_jobs.extend(jobs)

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"❌ {source_name} failed after {execution_time:.2f}s: {e}")

    return all_jobs


def process_jobs(jobs: List[Job]) -> None:
    """Process jobs through the pipeline: filter -> deduplicate -> store."""
    repository = JobRepository()

    total_fetched = len(jobs)
    filtered_count = 0
    duplicate_count = 0
    stored_count = 0

    logger.info(f"🔄 Processing {total_fetched} jobs through pipeline")

    for job in jobs[:settings.JOB_FETCH_LIMIT]:  # Limit total processing
        try:
            # Step 1: Filter
            if not passes_filter(job):
                filtered_count += 1
                continue

            # Step 2: Deduplicate
            if is_duplicate(job, repository):
                duplicate_count += 1
                continue

            # Step 3: Store
            if repository.insert_job(job):
                stored_count += 1
            else:
                logger.warning(f"Failed to store job: {job.title} at {job.company}")

        except Exception as e:
            logger.warning(f"Failed to process job {job.job_id}: {e}")
            continue

    logger.info("📊 Pipeline Summary:")
    logger.info(f"  Total fetched: {total_fetched}")

    if total_fetched > 0:
        logger.info(f"  Filtered out: {filtered_count} ({filtered_count/total_fetched:.2%})")
        logger.info(f"  Duplicates removed: {duplicate_count} ({duplicate_count/total_fetched:.2%})")
        logger.info(f"  Successfully stored: {stored_count} ({stored_count/total_fetched:.2%})")
    else:
        logger.info(f"  Filtered out: {filtered_count}")
        logger.info(f"  Duplicates removed: {duplicate_count}")
        logger.info(f"  Successfully stored: {stored_count}")


def main():
    logger.info("🚀 Starting Job Intelligence Engine")

    init_db()

    jobs = fetch_all_jobs()

    process_jobs(jobs)

    logger.info("🎯 Pipeline completed")


if __name__ == "__main__":
    main()