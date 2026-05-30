from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from app.dependencies import get_profile, get_repository
from app.schemas.job import JobItemResponse, RunDetailResponse, RunResponse
from pipeline.orchestrator import fetch_all_jobs, process_jobs
from storage.repository import JobRepository

router = APIRouter()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _execute_pipeline_run(run_id: str, repository: JobRepository, profile: dict) -> None:
    """Background task: run full pipeline and update job_runs row."""
    repository.update_run_status(run_id, "running", started_at=_now_iso())
    try:
        jobs = fetch_all_jobs()
        process_jobs(jobs, repository, profile)
        top_jobs = repository.get_top_jobs(limit=30)
        result = [
            {
                "job_id": j.job_id,
                "title": j.title,
                "company": j.company,
                "location": j.location,
                "url": j.url,
                "score": j.score,
                "match_type": getattr(j, "match_type", ""),
            }
            for j in top_jobs
        ]
        repository.update_run_status(
            run_id,
            "complete",
            completed_at=_now_iso(),
            result_json=json.dumps(result),
        )
    except Exception:
        repository.update_run_status(run_id, "failed", completed_at=_now_iso())


@router.post("/run-job-finder", response_model=RunResponse, status_code=202)
def run_job_finder(
    background_tasks: BackgroundTasks,
    repository: JobRepository = Depends(get_repository),
    profile: dict = Depends(get_profile),
):
    run_id = str(uuid.uuid4())
    repository.create_job_run(run_id)
    background_tasks.add_task(_execute_pipeline_run, run_id, repository, profile)
    return RunResponse(run_id=run_id, status="queued")


@router.get("/runs/{run_id}", response_model=RunDetailResponse)
def get_run(run_id: str, repository: JobRepository = Depends(get_repository)):
    run = repository.get_job_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    result = None
    if run["status"] == "complete" and run.get("result_json"):
        try:
            raw = json.loads(run["result_json"])
            result = [JobItemResponse(**item) for item in raw]
        except Exception:
            result = None

    return RunDetailResponse(
        run_id=run["run_id"],
        status=run["status"],
        created_at=run["created_at"] or "",
        started_at=run.get("started_at"),
        completed_at=run.get("completed_at"),
        result=result,
    )
