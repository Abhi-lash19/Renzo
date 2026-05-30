from fastapi import APIRouter, Depends

from app.dependencies import get_repository
from app.schemas.feedback import FeedbackRequest, FeedbackResponse
from storage.repository import JobRepository

router = APIRouter()


@router.post("/feedback", response_model=FeedbackResponse)
def record_feedback(
    body: FeedbackRequest,
    repository: JobRepository = Depends(get_repository),
):
    recorded = repository.record_interaction(body.job_id, body.action)
    return FeedbackResponse(recorded=recorded)
