from typing import Dict, List
from utils.text_utils import contains_term, normalize_text
from utils.logger import get_logger

logger = get_logger(__name__)


def generate_insight(job, profile: Dict[str, List[str]]) -> Dict[str, object]:
    """Generate a concise, structured insight summary for a job."""
    matched_skills = job.skills or []
    missing_skills = job.missing_skills or []

    combined_text = normalize_text(" ".join(filter(None, [job.title, job.company, job.description])))
    alignment_terms = [
        term.strip().lower()
        for term in profile.get("preferred_keywords", [])
        + profile.get("projects", [])
        + profile.get("experience", [])
        if term and term.strip()
    ]

    alignment_matches = [
        term
        for term in sorted(set(alignment_terms))
        if contains_term(combined_text, term)
    ]

    top_matched = matched_skills[:3]
    top_missing = missing_skills[:3]
    top_alignment = alignment_matches[:3]

    if matched_skills:
        why_match = f"Job aligns with your profile around {', '.join(top_matched)}."
    else:
        why_match = "This role has limited skill overlap with your current core profile."

    if top_alignment:
        why_match += f" It also matches keywords like {', '.join(top_alignment)}."

    if missing_skills:
        recommendation = (
            f"Strengthen {', '.join(top_missing)} before applying and highlight related experience."
        )
    elif top_alignment:
        recommendation = (
            f"Emphasize your work on {', '.join(top_alignment)} to improve relevance."
        )
    else:
        recommendation = "Focus your application on the matched skills and explain how they transfer to this job."

    insight = {
        "why_match": why_match,
        "missing_skills": missing_skills,
        "recommendation": recommendation,
        "alignment_keywords": top_alignment,
    }

    logger.debug(
        f"[INSIGHT] job_id={getattr(job, 'job_id', 'unknown')} "
        f"matched={len(matched_skills)} missing={len(missing_skills)} "
        f"alignment={top_alignment}"
    )
    return insight
