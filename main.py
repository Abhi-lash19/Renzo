from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Tuple
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.remotive_api import RemotiveFetcher
from pipeline.models import Job
from pipeline.filter import passes_filter
from pipeline.deduplicate import is_duplicate
from pipeline.scorer import score_job
from storage.repository import JobRepository
from utils.logger import get_logger
from utils.profile_loader import load_profile
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

    logger.info(f"📥 Total fetched across all sources: {len(all_jobs)} jobs")

    if len(all_jobs) == 0:
        logger.error("❌ CRITICAL: No jobs fetched from any source")
        logger.warning("💡 Check: API credentials, network connectivity, and SEARCH_KEYWORDS config")

    return all_jobs


def filter_jobs(jobs: List[Job], profile: dict, fallback: bool = False) -> Tuple[List[Job], int]:
    """Filter jobs and return only accepted jobs with rejection count."""
    accepted_jobs: List[Job] = []
    filtered_count = 0

    for job in jobs[: settings.JOB_FETCH_LIMIT]:
        passed, reason = passes_filter(job, profile, fallback=fallback)

        if passed:
            accepted_jobs.append(job)
        else:
            filtered_count += 1
            logger.info(f"❌ Filter rejected: {job.title or 'Unknown title'} | Reason: {reason}")

    return accepted_jobs, filtered_count


def deduplicate_jobs(jobs: List[Job], repository: JobRepository) -> Tuple[List[Job], int]:
    """Remove duplicate jobs based on hash and return unique jobs."""
    unique_jobs: List[Job] = []
    duplicate_count = 0

    for job in jobs:
        if is_duplicate(job, repository):
            duplicate_count += 1
        else:
            unique_jobs.append(job)

    logger.info(f"🧹 Duplicates removed: {duplicate_count}")
    return unique_jobs, duplicate_count


def store_jobs(jobs: List[Job], repository: JobRepository) -> List[Job]:
    """Store jobs in the repository and log each successful insert."""
    stored_jobs: List[Job] = []

    for job in jobs:
        if repository.insert_job(job):
            stored_jobs.append(job)
            logger.info(f"💾 Stored: {job.title} | Source: {job.source}")
        else:
            logger.warning(f"Failed to store job: {job.title} | Source: {job.source}")

    logger.info(f"📦 Total stored jobs: {len(stored_jobs)}")

    if len(stored_jobs) == 0:
        logger.critical("No jobs stored. Possible causes: filtering too strict / DB issue")

    return stored_jobs


def score_stored_jobs(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    """Score all stored jobs and persist the score to the database."""
    scored_count = 0

    for job in jobs:
        score_job(job, profile)

        if repository.update_job_score(job.job_id, job.score):
            scored_count += 1
            logger.info(f"⭐ Scored: {job.title} → score: {job.score:.2f}")

            if job.score == 0:
                logger.warning(f"⚠️ Zero score for stored job: {job.title}")
        else:
            logger.error(f"Failed to persist score for job: {job.title}")

    logger.info(f"📈 Total scored jobs: {scored_count}/{len(jobs)}")
    return scored_count


def print_top_jobs(repository: JobRepository) -> None:
    """Retrieve and print top jobs from the DB."""
    top_jobs = repository.get_top_jobs(limit=30)

    if not top_jobs:
        logger.error("No jobs available for output")
        return

    logger.info("🏆 Top Jobs:")
    for index, job in enumerate(top_jobs[:5], start=1):
        logger.info(f"{index}. Title: {job.title}")
        logger.info(f"   Company: {job.company}")
        logger.info(f"   Score: {job.score:.2f}")
        logger.info(f"   Source: {job.source}")

    logger.info(f"Displayed top {min(len(top_jobs), 5)} of {len(top_jobs)} jobs")


def process_jobs(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    """Process jobs through the pipeline using ordered stages."""
    total_fetched = len(jobs)
    logger.info(f"🔄 Processing {total_fetched} jobs through pipeline")

    if total_fetched == 0:
        logger.warning("⚠️ Pipeline: No jobs to process")
        return 0

    filtered_jobs, filtered_count = filter_jobs(jobs, profile)

    if len(filtered_jobs) < 10 and total_fetched > 0:
        logger.warning("⚠️ Fallback mode activated: relaxing filter criteria")
        filtered_jobs, filtered_count = filter_jobs(jobs, profile, fallback=True)

    unique_jobs: List[Job] = []
    duplicate_count = 0
    for job in filtered_jobs:
        if is_duplicate(job, repository):
            duplicate_count += 1
        else:
            unique_jobs.append(job)

    logger.info(f"🧹 Unique jobs after deduplication: {len(unique_jobs)}")
    logger.info(f"  Duplicates removed: {duplicate_count}")

    stored_jobs: List[Job] = []
    for job in unique_jobs:
        if repository.insert_job(job):
            stored_jobs.append(job)
        else:
            logger.warning(f"Failed to store job after deduplication: {job.title}")

    logger.info(f"📦 Jobs stored: {len(stored_jobs)}")

    scored_count = 0
    for job in stored_jobs:
        score_job(job, profile)
        if repository.update_job_score(job.job_id, job.score):
            scored_count += 1
            logger.info(f"⭐ Scored: {job.title} → score: {job.score:.2f}")
        else:
            logger.error(f"Failed to persist score for job: {job.title}")

    logger.info("📊 Pipeline Summary:")
    logger.info(f"  Fetched: {total_fetched} jobs")
    logger.info(f"  Filtered out: {filtered_count} jobs")
    logger.info(f"  Duplicates removed: {duplicate_count} jobs")
    logger.info(f"  Stored: {len(stored_jobs)} jobs")
    logger.info(f"  Scored: {scored_count} jobs")

    if total_fetched > 0:
        logger.info(f"  Filtered out rate: {filtered_count/total_fetched:.2%}")
        logger.info(f"  Stored rate: {len(stored_jobs)/total_fetched:.2%}")
        logger.info(f"  Scored rate: {scored_count/total_fetched:.2%}")

    return len(stored_jobs)


def main():
    logger.info("🚀 Starting Job Intelligence Engine")

    init_db()
    profile = load_profile()
    logger.info(
        f"Loaded profile: core={len(profile.get('core_skills', []))}, "
        f"secondary={len(profile.get('secondary_skills', []))}, "
        f"roles={len(profile.get('preferred_roles', []))}, "
        f"preferred_keywords={len(profile.get('preferred_keywords', []))}, "
        f"bonus_keywords={len(profile.get('bonus_keywords', []))}, "
        f"exclude_keywords={len(profile.get('exclude_keywords', []))}"
    )

    jobs = fetch_all_jobs()
    if len(jobs) == 0:
        logger.error("❌ PIPELINE ABORTED: No jobs fetched from any source")
        return

    repository = JobRepository()
    stored_count = process_jobs(jobs, repository, profile)

    if stored_count == 0:
        logger.error("❌ CRITICAL: No jobs were stored in the database")
        logger.warning("Possible causes:")
        logger.warning("  1. All fetched jobs were filtered out (check filter criteria)")
        logger.warning("  2. Database error (check permissions, disk space)")
        logger.warning("  3. Jobs contain missing required fields")
    else:
        print_top_jobs(repository)

    logger.info("🎯 Pipeline completed")


if __name__ == "__main__":
    main()