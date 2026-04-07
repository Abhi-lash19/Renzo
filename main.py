from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.remotive_api import RemotiveFetcher
from pipeline.models import Job
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
            start_time = time.time()

            try:
                jobs = future.result()
                execution_time = time.time() - start_time
                logger.info(f"✅ {source_name}: {len(jobs)} jobs fetched in {execution_time:.2f}s")
                all_jobs.extend(jobs)

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"❌ {source_name} failed after {execution_time:.2f}s: {e}")

    return all_jobs


def main():
    logger.info("🚀 Starting Job Intelligence Engine")

    init_db()

    jobs = fetch_all_jobs()

    logger.info(f"📊 Total jobs fetched: {len(jobs)}")

    logger.info("🔍 Sample jobs:")
    for job in jobs[:5]:
        logger.info(f"{job.title} | {job.company} | {job.source}")

    logger.info("🎯 Fetch phase completed")


if __name__ == "__main__":
    main()