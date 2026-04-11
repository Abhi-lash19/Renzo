import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

from config.settings import settings
from fetchers.adzuna_api import AdzunaFetcher
from fetchers.indeed_rss import IndeedRSSFetcher
from fetchers.remotive_api import RemotiveFetcher
from intelligence.resume_enhancer import generate_insight
from intelligence.skill_gap import compute_skill_gap
from pipeline.deduplicate import is_duplicate
from pipeline.filter import passes_filter
from pipeline.models import Job
from pipeline.scorer import score_job
from storage.db import init_db
from storage.repository import JobRepository
from utils.logger import get_logger
from utils.profile_loader import load_profile

logger = get_logger(__name__)
OUTPUT_DIR = Path("output")


def _stage_log(stage: str, started_at: float, message: str) -> None:
    elapsed = time.perf_counter() - started_at
    logger.info(f"[{stage}] {message} | {elapsed:.2f}s")


def fetch_all_jobs() -> List[Job]:
    """Fetch jobs from all sources concurrently with timing and error handling."""
    sources = [
        IndeedRSSFetcher(),
        AdzunaFetcher(),
        RemotiveFetcher()
    ]

    all_jobs: List[Job] = []

    try:
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            future_to_source = {
                executor.submit(source.fetch_and_normalize): (source.__class__.__name__, time.perf_counter())
                for source in sources
            }
            for future in as_completed(future_to_source):
                source_name, source_started = future_to_source[future]
                try:
                    jobs = future.result()
                    execution_time = time.perf_counter() - source_started
                    logger.info(f"✅ {source_name}: {len(jobs)} jobs fetched in {execution_time:.2f}s")
                    all_jobs.extend(jobs)
                except Exception as e:
                    logger.exception(f"❌ {source_name} failed: {e}")
    except Exception as e:
        logger.exception(f"Threadpool error: {e}")

    logger.info(f"📥 Total fetched across all sources: {len(all_jobs)} jobs")
    if len(all_jobs) == 0:
        logger.error("❌ CRITICAL: No jobs fetched from any source")
    return all_jobs

def filter_jobs(jobs: List[Job], profile: dict, fallback: bool = False) -> Tuple[List[Job], int, float]:
    accepted_jobs: List[Job] = []
    filtered_count = 0
    total_score = 0.0
    
    threshold = 3 if fallback else 4
    limit_jobs = jobs[: settings.JOB_FETCH_LIMIT]

    for job in limit_jobs:
        try:
            passed, reason, filter_score = passes_filter(job, profile, threshold=threshold)
            total_score += filter_score
            if passed:
                accepted_jobs.append(job)
            else:
                filtered_count += 1
        except Exception as e:
            logger.exception(f"Error filtering job: {e}")
            filtered_count += 1

    # Fallback logic
    if not fallback and len(accepted_jobs) < 20:
        logger.info(f"Only {len(accepted_jobs)} passed limit. Engaging fallback threshold=3")
        accepted_jobs = []
        filtered_count = 0
        total_score = 0.0
        for job in limit_jobs:
            try:
                passed, reason, filter_score = passes_filter(job, profile, threshold=3)
                total_score += filter_score
                if passed:
                    accepted_jobs.append(job)
                else:
                    filtered_count += 1
            except Exception:
                filtered_count += 1

    evaluated_count = len(limit_jobs)
    avg_score = (total_score / evaluated_count) if evaluated_count > 0 else 0.0
    return accepted_jobs, filtered_count, avg_score

def deduplicate_jobs(jobs: List[Job], repository: JobRepository) -> Tuple[List[Job], int]:
    unique_jobs: List[Job] = []
    duplicate_count = 0
    
    # clear local memory for fuzzy match this run
    import pipeline.deduplicate
    pipeline.deduplicate._local_jobs = [] 

    for job in jobs:
        try:
            if is_duplicate(job, repository):
                duplicate_count += 1
            else:
                unique_jobs.append(job)
        except Exception as e:
            logger.exception(f"Error deduplicating job: {e}")
            unique_jobs.append(job)
    return unique_jobs, duplicate_count

def store_jobs(jobs: List[Job], repository: JobRepository) -> List[Job]:
    stored_jobs: List[Job] = []
    for job in jobs:
        try:
            # Skip if missing job_id (SAFETY CHECK)
            if not job.job_id:
                logger.warning(f"Skipping job with missing ID: {job.title}")
                continue

            # Ensure globally unique job_id (CRITICAL FIX)
            job.job_id = f"{job.source}_{job.job_id}"

            if repository.insert_job(job):
                repository.insert_skills(job.job_id, job.skills)
                stored_jobs.append(job)
        except Exception as e:
            logger.exception(f"Storage failed for job: {e}")
    return stored_jobs


def enrich_jobs(jobs: List[Job], profile: dict) -> Tuple[List[Job], float]:
    """
    Enrich jobs with scoring.

    IMPORTANT:
    - Skill extraction MUST NOT happen here.
    - build_match_data() is the single source of truth and should already be executed before this step.
    """
    enriched_jobs: List[Job] = []
    total_score = 0.0

    for job in jobs:
        try:
            # SAFETY: Ensure match_data exists (fail fast if pipeline order breaks)
            if not getattr(job, "match_data", None):
                logger.error(
                    f"[PIPELINE_ERROR] Missing match_data before scoring "
                    f"job_id={getattr(job, 'job_id', 'unknown')} title={getattr(job, 'title', '')}"
                )
                raise ValueError("match_data must be built before scoring")

            score = score_job(job, profile)
            total_score += score
            enriched_jobs.append(job)

        except Exception as e:
            logger.exception(
                f"Error enriching job job_id={getattr(job, 'job_id', 'unknown')} "
                f"title={getattr(job, 'title', '')}: {e}"
            )

    avg_score = (total_score / len(enriched_jobs)) if enriched_jobs else 0.0
    return enriched_jobs, avg_score


def generate_intelligence(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    intelligence_count = 0
    for job in jobs:
        try:
            compute_skill_gap(job, profile)
            job.insight = generate_insight(job, profile)
            skills_saved = repository.insert_skills(job.job_id, job.skills)
            missing_saved = repository.insert_missing_skills(job.job_id, job.missing_skills)
            score_saved = repository.update_job_score(job.job_id, job.score)
            if skills_saved and missing_saved and score_saved:
                intelligence_count += 1
        except Exception as e:
            logger.exception(f"Error generating intelligence for job {getattr(job, 'job_id', 'unknown')}: {e}")
    return intelligence_count


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
        (OUTPUT_DIR / "job_report.txt").write_text("\n".join(report_lines), encoding="utf-8")

        aggregated_gap: Dict[str, int] = {}
        for job in top_jobs:
            for skill in job.missing_skills:
                aggregated_gap[skill] = aggregated_gap.get(skill, 0) + 1
        gap_lines = [
            f"{skill}: missing in {count} jobs"
            for skill, count in sorted(aggregated_gap.items(), key=lambda item: (-item[1], item[0]))
        ]
        (OUTPUT_DIR / "skill_gap_report.txt").write_text(
            "\n".join(gap_lines) if gap_lines else "No missing skills detected in top jobs.",
            encoding="utf-8",
        )

        for job in top_jobs[:10]:
            safe_job_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in job.job_id)
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
                "\n".join(lines),
                encoding="utf-8",
            )

        logger.info(f"[OUTPUT] Generated reports for {len(top_jobs)} jobs in {OUTPUT_DIR}")
    except Exception as e:
        logger.exception(f"Error exporting outputs: {e}")

def score_stored_jobs(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    """
    Re-score already stored jobs.

    IMPORTANT:
    - Skill extraction MUST NOT happen here.
    - match_data must already exist.
    """
    scored_count = 0

    for job in jobs:
        try:
            if not getattr(job, "match_data", None):
                logger.error(
                    f"[PIPELINE_ERROR] Missing match_data before re-scoring "
                    f"job_id={getattr(job, 'job_id', 'unknown')}"
                )
                raise ValueError("match_data must be built before scoring")

            compute_skill_gap(job, profile)
            job.insight = generate_insight(job, profile)
            score_job(job, profile)

            if repository.update_job_score(job.job_id, job.score):
                scored_count += 1

        except Exception as e:
            logger.exception(
                f"Error scoring/updating job job_id={getattr(job, 'job_id', 'unknown')}: {e}"
            )

    return scored_count

def print_top_jobs(repository: JobRepository, profile: dict) -> None:
    try:
        top_jobs = repository.get_top_jobs(limit=30)
        if not top_jobs:
            logger.error("No jobs available for output")
            return

        logger.info("🏆 Top Jobs:")
        for index, job in enumerate(top_jobs[:5], start=1):
            try:
                if not getattr(job, "match_data", None):
                    logger.warning(
                        f"[PIPELINE_WARNING] match_data missing during print for job_id={job.job_id}"
                    )

                compute_skill_gap(job, profile)
                insight = generate_insight(job, profile)

                logger.info(f"{index}. {job.title} (Score: {job.score:.2f})")
                logger.info(f"   Company: {job.company}")
                
                matched_str = ', '.join(job.skills) if job.skills else 'None'
                missing_str = ', '.join(getattr(job, 'missing_skills', [])) or 'None'
                
                logger.info(f"   Matched: {matched_str}")
                logger.info(f"   Missing: {missing_str}")
                logger.info(f"   Insight: {insight.get('recommendation', '')}")
                logger.info(f"   Source: {job.source}")
            except Exception as e:
                logger.exception(f"Error printing job {index}: {e}")
                
        logger.info(f"Displayed top {min(len(top_jobs), 5)} of {len(top_jobs)} jobs")
    except Exception as e:
        logger.exception(f"Error printing top jobs: {e}")

def process_jobs(jobs: List[Job], repository: JobRepository, profile: dict) -> int:
    total_fetched = len(jobs)
    if total_fetched == 0:
        return 0

    try:
        filter_started = time.perf_counter()
        filtered_jobs, filtered_count, avg_filter_score = filter_jobs(jobs, profile)
        _stage_log("FILTER_TIME", filter_started, "finished filtering")
        logger.info(f"[FILTER] total={total_fetched} accepted={len(filtered_jobs)} rejected={filtered_count} avg_score={avg_filter_score:.1f}")

        dedup_started = time.perf_counter()
        unique_jobs, duplicate_count = deduplicate_jobs(filtered_jobs, repository)
        _stage_log("DEDUP_TIME", dedup_started, "finished dedup")
        logger.info(f"[DEDUP] unique={len(unique_jobs)} duplicates={duplicate_count}")

        score_started = time.perf_counter()
        enriched_jobs, avg_eval_score = enrich_jobs(unique_jobs, profile)
        _stage_log("SCORE_TIME", score_started, "finished scoring")
        logger.info(f"[SCORE] avg_score={avg_eval_score:.1f}")

        store_started = time.perf_counter()
        stored_jobs = store_jobs(enriched_jobs, repository)
        _stage_log("STORE_TIME", store_started, f"stored={len(stored_jobs)}")

        intelligence_started = time.perf_counter()
        intelligence_count = generate_intelligence(stored_jobs, repository, profile)
        _stage_log("INTEL_TIME", intelligence_started, f"intelligence={intelligence_count}")

        logger.info(
            f"Processed {total_fetched} jobs -> {len(filtered_jobs)} relevant -> "
            f"{len(unique_jobs)} unique -> {len(stored_jobs)} stored -> {intelligence_count} intelligent"
        )
        return len(stored_jobs)
    except Exception as e:
        logger.exception(f"Fatal error in process_jobs: {e}")
        return 0

def main():
    try:
        init_db()
        profile = load_profile()
        jobs = fetch_all_jobs()
        if len(jobs) == 0:
            return

        repository = JobRepository()
        stored_count = process_jobs(jobs, repository, profile)

        if stored_count > 0:
            export_outputs(repository, profile)

        logger.info("🎯 Pipeline completed")
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")

if __name__ == "__main__":
    main()
