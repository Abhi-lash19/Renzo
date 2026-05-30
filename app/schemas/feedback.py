from typing import Literal
from pydantic import BaseModel


class FeedbackRequest(BaseModel):
    job_id: str
    action: Literal["applied", "ignored", "viewed"]


class FeedbackResponse(BaseModel):
    recorded: bool
