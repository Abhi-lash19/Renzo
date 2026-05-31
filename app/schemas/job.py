from __future__ import annotations
from pydantic import BaseModel


class RunResponse(BaseModel):
    run_id: str
    status: str


class JobItemResponse(BaseModel):
    job_id: str
    title: str
    company: str
    location: str
    url: str
    score: float
    match_type: str


class RunDetailResponse(BaseModel):
    run_id: str
    status: str
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    result: list[JobItemResponse] | None = None


class JobTopResponse(BaseModel):
    """Job with full score breakdown for GET /v1/jobs/top."""
    job_id: str
    title: str
    company: str
    location: str
    url: str
    score: float = 0.0
    match_type: str = ""
    fused_score: float = 0.0
    vector_score: float = 0.0
    retrieval_mode: str = "keyword_only"  # "hybrid" | "keyword_only"
