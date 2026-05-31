"""
evaluation/datasets/ground_truth.py — Human-judged relevance labels per profile.

Relevance scale:
  2 = highly relevant (should be in top results — human would definitely apply)
  1 = borderline / stretch (worth considering — human might apply)
  0 = not relevant (human would not apply to this job)

These represent HUMAN JUDGMENT, independent of what the current system scores.
"""

GROUND_TRUTH: dict = {
    "junior-python-backend": {
        "eval_job_j001": 2,  # Python Backend Engineer — perfect match
        "eval_job_j002": 2,  # Backend Developer Python/FastAPI — exact stack
        "eval_job_j003": 2,  # Software Engineer Backend — strong match
        "eval_job_j004": 2,  # API Engineer — FastAPI + PostgreSQL + REST
        "eval_job_j005": 2,  # Python Microservices — full stack match
        "eval_job_j006": 1,  # Flask — adjacent to FastAPI, still Python backend
        "eval_job_j007": 1,  # Django REST — Python backend, different framework
        "eval_job_j008": 1,  # Backend Go/Python — has Python component
        "eval_job_j009": 0,  # AWS Cloud Engineer — wrong role type
        "eval_job_j010": 0,  # DevOps Terraform — wrong role type
        "eval_job_j011": 0,  # Platform Engineer — wrong role type
        "eval_job_j012": 0,  # GCP Cloud — wrong role type
        "eval_job_j013": 0,  # Azure DevOps — wrong role type
        "eval_job_j014": 1,  # Full Stack Python/React — Python backend relevant
        "eval_job_j015": 1,  # Full Stack Software Engineer — backend part relevant
        "eval_job_j016": 0,  # React Frontend — excluded keyword
        "eval_job_j017": 0,  # iOS Mobile — excluded keyword
        "eval_job_j018": 0,  # WordPress PHP — wrong tech
        "eval_job_j019": 0,  # Sales Engineer — wrong domain
        "eval_job_j020": 0,  # ML Research — wrong domain
    },
    "aws-cloud-engineer": {
        "eval_job_j001": 0,  # Python Backend — not cloud/devops
        "eval_job_j002": 0,  # Backend FastAPI — not cloud role
        "eval_job_j003": 0,  # Python Services — not cloud primary
        "eval_job_j004": 0,  # API Engineer — not cloud/devops
        "eval_job_j005": 0,  # Python Microservices — not cloud role
        "eval_job_j006": 0,  # Flask Backend — not cloud role
        "eval_job_j007": 0,  # Django REST — not cloud role
        "eval_job_j008": 0,  # Go/Python Backend — not cloud role
        "eval_job_j009": 2,  # AWS Cloud Engineer — perfect match
        "eval_job_j010": 2,  # DevOps Terraform/AWS — exact role
        "eval_job_j011": 2,  # Platform Engineer K8s/AWS — strong match
        "eval_job_j012": 1,  # GCP Cloud — adjacent to AWS, same domain
        "eval_job_j013": 1,  # Azure DevOps — adjacent, transferable
        "eval_job_j014": 0,  # Full Stack — not cloud role
        "eval_job_j015": 0,  # Full Stack Engineer — not cloud role
        "eval_job_j016": 0,  # React Frontend — wrong
        "eval_job_j017": 0,  # iOS — wrong
        "eval_job_j018": 0,  # WordPress — wrong
        "eval_job_j019": 0,  # Sales — wrong
        "eval_job_j020": 0,  # ML Research — wrong
    },
    "fullstack-developer": {
        "eval_job_j001": 1,  # Python Backend — backend part relevant, missing frontend
        "eval_job_j002": 1,  # Backend FastAPI — backend relevant only
        "eval_job_j003": 0,  # Backend — too backend-only, no frontend
        "eval_job_j004": 0,  # API Engineer — too backend-only
        "eval_job_j005": 0,  # Python Microservices — missing frontend
        "eval_job_j006": 0,  # Flask Backend — missing frontend component
        "eval_job_j007": 0,  # Django REST — missing frontend
        "eval_job_j008": 0,  # Go/Python — wrong language + missing frontend
        "eval_job_j009": 0,  # AWS Cloud — wrong role type
        "eval_job_j010": 0,  # DevOps — wrong role type
        "eval_job_j011": 0,  # Platform Engineer — wrong role type
        "eval_job_j012": 0,  # GCP Cloud — wrong role type
        "eval_job_j013": 0,  # Azure DevOps — wrong role type
        "eval_job_j014": 2,  # Full Stack Python/React — perfect match
        "eval_job_j015": 2,  # Full Stack Software Engineer — excellent match
        "eval_job_j016": 1,  # React Frontend — has frontend component
        "eval_job_j017": 0,  # iOS — excluded
        "eval_job_j018": 0,  # WordPress — excluded tech
        "eval_job_j019": 0,  # Sales Engineer — wrong domain
        "eval_job_j020": 0,  # ML Research — wrong domain
    },
}


def get_relevant_job_ids(profile_id: str, min_relevance: int = 1) -> list:
    """Return job IDs with relevance >= min_relevance for the given profile."""
    labels = GROUND_TRUTH.get(profile_id, {})
    return [jid for jid, rel in labels.items() if rel >= min_relevance]


def get_highly_relevant_job_ids(profile_id: str) -> list:
    """Return job IDs with relevance == 2 for the given profile."""
    return get_relevant_job_ids(profile_id, min_relevance=2)
