import difflib
from typing import TYPE_CHECKING

from storage.repository import JobRepository
from utils.hash_utils import build_job_identity
from utils.logger import get_logger
from utils.text_utils import normalize_text

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)

_seen_hashes = []
_local_jobs = []  # Local memory cache for conservative fuzzy matching within run


def _build_fuzzy_fingerprint(job: "Job") -> dict[str, str]:
    return {
        "title": normalize_text(getattr(job, "title", "")),
        "company": normalize_text(getattr(job, "company", "")),
        "location": normalize_text(getattr(job, "location", "")),
        "source": normalize_text(getattr(job, "source", "")),
    }


def _log_duplicate(job: "Job", reason: str) -> None:
    logger.debug(
        f"[DEDUP_DEBUG] title={getattr(job, 'title', '')[:60]} | "
        f"company={getattr(job, 'company', '')[:40]} | why_duplicate={reason}"
    )


def is_duplicate(job: "Job", repository: JobRepository) -> bool:
    """
    Check if the job is a duplicate using preferred identity keys first,
    then a conservative fuzzy fallback only when identifiers are missing.
    """
    has_fallback_fields = bool(getattr(job, "title", None) and getattr(job, "company", None))
    if not (getattr(job, "url", None) or getattr(job, "job_id", None) or has_fallback_fields):
        logger.debug(
            f"Invalid dedup input: job_id={getattr(job, 'job_id', 'unknown')} "
            f"title={getattr(job, 'title', '') or 'missing'} "
            f"company={getattr(job, 'company', '') or 'missing'}"
        )
        return False

    identity_kind, identity_key = build_job_identity(job)
    if any(seen_job.get("identity_key") == identity_key for seen_job in _local_jobs):
        _log_duplicate(job, f"local exact {identity_kind} match")
        return True

    if repository.hash_exists(identity_key):
        _log_duplicate(job, f"stored exact {identity_kind} match")
        return True

    fingerprint = _build_fuzzy_fingerprint(job)
    combined_fingerprint = " | ".join(
        [fingerprint["title"], fingerprint["company"], fingerprint["location"], fingerprint["source"]]
    )

    if identity_kind == "hash":
        for seen_job in _local_jobs:
            same_company = seen_job["company"] == fingerprint["company"]
            same_location = seen_job["location"] == fingerprint["location"]
            same_source = seen_job["source"] == fingerprint["source"]
            if not (same_company and same_location and same_source):
                continue

            ratio = difflib.SequenceMatcher(
                None,
                combined_fingerprint,
                seen_job["fingerprint"],
            ).ratio()
            if ratio >= 0.9:
                _log_duplicate(
                    job,
                    f"fuzzy fallback match ratio={ratio:.2f} company/location/source aligned",
                )
                return True

    if len(_seen_hashes) < 5:
        _seen_hashes.append(identity_key)
        logger.debug(f"Sample dedup hash [{len(_seen_hashes)}]: {identity_key}")

    _local_jobs.append({**fingerprint, "fingerprint": combined_fingerprint, "identity_key": identity_key})

    if not repository.insert_hash(identity_key):
        logger.warning(
            f"Unable to record dedup identity for job_id={getattr(job, 'job_id', 'unknown')} "
            f"title={getattr(job, 'title', '')} identity_type={identity_kind}"
        )
        return False

    return False
