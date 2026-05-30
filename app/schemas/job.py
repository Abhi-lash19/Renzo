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
