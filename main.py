"""
Renzo — entry point.

Responsibilities:
  - fetch jobs from all sources (concurrent)
  - call the pipeline orchestrator
  - write output files
  - manage DB lifecycle

Pipeline orchestration lives in pipeline/orchestrator.py.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List

from config.settings import settings
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.remotive_api import RemotiveFetcher
from intelligence.resume_enhancer import generate_insight
from pipeline.models import Job
from pipeline.orchestrator import process_jobs
from storage.db import init_db
from storage.repository import JobRepository
from utils.logger import get_logger
from utils.profile_loader import load_profile

logger = get_logger(__name__)
OUTPUT_DIR = Path("output")


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_all_jobs() -> List[Job]:
    """Fetch jobs from all sources concurrently."""
    sources = [
        IndeedRSSFetcher(),
        AdzunaFetcher(),
        RemotiveFetcher(),
    ]

    all_jobs: List[Job] = []

    try:
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            future_to_source = {
                executor.submit(source.fetch_and_normalize): (
                    source.__class__.__name__,
                    time.perf_counter(),
                )
                for source in sources
            }
            for future in as_completed(future_to_source):
                source_name, source_started = future_to_source[future]
                try:
                    jobs = future.result()
                    elapsed = time.perf_counter() - source_started
                    logger.info(
                        f"✅ {source_name}: {len(jobs)} jobs fetched in {elapsed:.2f}s"
                    )
                    all_jobs.extend(jobs)
                except Exception as e:
                    logger.exception(f"❌ {source_name} failed: {e}")
    except Exception as e:
        logger.exception(f"Threadpool error: {e}")

    logger.info(f"📥 Total fetched across all sources: {len(all_jobs)} jobs")
    if not all_jobs:
        logger.error("❌ CRITICAL: No jobs fetched from any source")
    return all_jobs


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _job_to_dict(job: Job) -> Dict[str, object]:
    return {
        "job_id": job.job_id,
        "title": job.title,
        "company": job.company,
        "location": job.location,
        "url": job.url,
        "source": job.source,
        "posted_at": job.posted_at.isoformat() if job.posted_at else None,
        "score": job.score,
        "matched_skills": job.skills,
        "missing_skills": job.missing_skills,
        "score_breakdown": getattr(job, "score_breakdown", {}),
    }


def export_outputs(repository: JobRepository, profile: dict) -> None:
    try:
        top_jobs = repository.get_top_jobs(limit=30)
        if not top_jobs:
            logger.error("[OUTPUT] No jobs available for output")
            return

        OUTPUT_DIR.mkdir(exist_ok=True)

        for job in top_jobs:
            if not getattr(job, "insight", None):
                job.insight = generate_insight(job, profile)

        (OUTPUT_DIR / "top_jobs.json").write_text(
            json.dumps([_job_to_dict(job) for job in top_jobs], indent=2),
            encoding="utf-8",
        )

        report_lines = []
        for index, job in enumerate(top_jobs[:10], start=1):
            report_lines.append(
                f"{index}. {job.title} | {job.company} | score={job.score:.2f}\n"
                f"   matched: {', '.join(job.skills) or 'none'}\n"
                f"   missing: {', '.join(job.missing_skills) or 'none'}\n"
                f"   source: {job.source}\n"
                f"   url: {job.url}\n"
            )
        (OUTPUT_DIR / "job_report.txt").write_text(
            "\n".join(report_lines), encoding="utf-8"
        )

        aggregated_gap: Dict[str, int] = {}
        for job in top_jobs:
            for skill in job.missing_skills:
                aggregated_gap[skill] = aggregated_gap.get(skill, 0) + 1
        gap_lines = [
            f"{skill}: missing in {count} jobs"
            for skill, count in sorted(
                aggregated_gap.items(), key=lambda item: (-item[1], item[0])
            )
        ]
        (OUTPUT_DIR / "skill_gap_report.txt").write_text(
            "\n".join(gap_lines) if gap_lines else "No missing skills detected in top jobs.",
            encoding="utf-8",
        )

        for job in top_jobs[:10]:
            safe_job_id = "".join(
                ch if ch.isalnum() or ch in "-_" else "_" for ch in job.job_id
            )
            lines = [
                f"Job: {job.title} at {job.company}",
                f"Why match: {job.insight.get('why_match', '')}",
                f"Recommendation: {job.insight.get('recommendation', '')}",
                "",
                "Summary suggestions:",
                *job.insight.get("summary_suggestions", []),
                "",
                "Skill highlights:",
                *job.insight.get("skill_highlights", []),
                "",
                "Project suggestions:",
                *job.insight.get("project_suggestions", []),
            ]
            (OUTPUT_DIR / f"resume_suggestions_{safe_job_id}.txt").write_text(
                "\n".join(lines), encoding="utf-8"
            )

        logger.info(
            f"[OUTPUT] Generated reports for {len(top_jobs)} jobs in {OUTPUT_DIR}"
        )
    except Exception as e:
        logger.exception(f"Error exporting outputs: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    from storage.db_manager import db_manager

    try:
        init_db()
        profile = load_profile()
        jobs = fetch_all_jobs()
        if not jobs:
            return

        repository = JobRepository()
        stored_count = process_jobs(jobs, repository, profile)

        if stored_count > 0:
            export_outputs(repository, profile)

        logger.info("🎯 Pipeline completed")
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
    finally:
        db_manager.shutdown()


if __name__ == "__main__":
    main()
