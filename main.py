from concurrent.futures import ThreadPoolExecutor, as_completed
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.remotive_api import RemotiveFetcher
from utils.logger import get_logger
from storage.db import init_db

logger = get_logger(__name__)


def fetch_all_jobs():
    sources = [
        IndeedRSSFetcher(),
        AdzunaFetcher(),
        RemotiveFetcher()
    ]

    all_jobs = []

    with ThreadPoolExecutor(max_workers=len(sources)) as executor:
        future_to_source = {
            executor.submit(source.fetch_and_normalize): source.__class__.__name__
            for source in sources
        }

        for future in as_completed(future_to_source):
            source_name = future_to_source[future]

            try:
                jobs = future.result()
                logger.info(f"✅ {source_name}: {len(jobs)} jobs fetched")
                all_jobs.extend(jobs)

            except Exception as e:
                logger.error(f"❌ {source_name} failed: {e}")

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