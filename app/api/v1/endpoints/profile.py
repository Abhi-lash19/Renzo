"""
Profile endpoints — upload resume, retrieve, and update user profile.
"""
from __future__ import annotations

import json
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.auth import get_current_user
from app.dependencies import get_repository
from app.schemas.profile import ProfileResponse, ProfileUpdateRequest
from resume.builder import build_profile, recompute_profile_skills
from resume.extractor import extract_text
from resume.parser import parse_resume
from storage.repository import JobRepository
from utils.logger import get_logger
from utils.profile_loader import load_profile

router = APIRouter(prefix="/profile", tags=["profile"])
logger = get_logger(__name__)

_MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


def _to_response(profile: dict, source: str = "file") -> ProfileResponse:
    return ProfileResponse(
        name=profile.get("name", ""),
        role=profile.get("role", ""),
        experience_level=profile.get("experience_level", "0-2 years"),
        core_skills=profile.get("core_skills", []),
        secondary_skills=profile.get("secondary_skills", []),
        all_skills=profile.get("all_skills", []),
        weighted_skills=profile.get("weighted_skills", {}),
        cloud=profile.get("cloud", []),
        devops=profile.get("devops", []),
        projects=profile.get("projects", []),
        experience=profile.get("experience", []),
        preferred_roles=profile.get("preferred_roles", []),
        target_roles=profile.get("target_roles", []),
        exclude_keywords=profile.get("exclude_keywords", []),
        bonus_keywords=profile.get("bonus_keywords", []),
        preferred_keywords=profile.get("preferred_keywords", []),
        location=profile.get("location", ""),
        remote_preferred=profile.get("remote_preferred", True),
        source=profile.get("source", source),
    )


@router.post("/upload", response_model=ProfileResponse, status_code=201)
async def upload_resume(
    file: Annotated[UploadFile, File(description="PDF, DOCX, or TXT resume file")],
    repository: JobRepository = Depends(get_repository),
    current_user: dict = Depends(get_current_user),
):
    """Upload a resume and extract a structured profile."""
    filename = file.filename or "resume.txt"
    file_bytes = await file.read()

    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    if len(file_bytes) > _MAX_FILE_SIZE_BYTES:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 5 MB.")

    logger.info(f"[PROFILE] Upload: filename={filename} size={len(file_bytes)}")

    try:
        raw_text = extract_text(file_bytes, filename)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    if not raw_text.strip():
        raise HTTPException(status_code=422, detail="Could not extract readable text.")

    parsed = parse_resume(raw_text)
    profile = build_profile(parsed)

    user_id = str(current_user.get("sub") or current_user.get("id", "anonymous"))
    repository.upsert_profile(
        user_id=user_id,
        profile_json=json.dumps(profile),
        raw_text=raw_text,
        source="upload",
        name=profile.get("name", ""),
        role=profile.get("role", ""),
        experience_level=profile.get("experience_level", "0-2 years"),
    )

    # Phase 5: generate and store profile embedding when enabled
    from config import settings as _settings_module
    if _settings_module.settings.EMBEDDINGS_ENABLED:
        try:
            from pipeline.embedder import build_profile_text, get_embedding_provider
            provider = get_embedding_provider()
            profile_text = build_profile_text(profile)
            if profile_text:
                profile_emb = provider.embed(profile_text)
                repository.store_profile_embedding(user_id, profile_emb)
                logger.info(f"[PROFILE] Stored embedding for user_id={user_id[:8]}...")
        except Exception as e:
            logger.warning(f"[PROFILE] Profile embedding failed (non-fatal): {e}")

    return _to_response(profile, source="upload")


@router.get("/", response_model=ProfileResponse)
def get_profile(
    repository: JobRepository = Depends(get_repository),
    current_user: dict = Depends(get_current_user),
):
    """Return the current user's profile (DB upload or file fallback)."""
    user_id = str(current_user.get("sub") or current_user.get("id", "anonymous"))
    stored = repository.get_profile_by_user(user_id)

    if stored and stored.get("profile_json"):
        try:
            profile = json.loads(stored["profile_json"])
            profile["source"] = stored.get("source", "upload")
            return _to_response(profile)
        except Exception as e:
            logger.warning(f"[PROFILE] Failed to parse stored profile JSON: {e}")

    file_profile = load_profile()
    file_profile["source"] = "file"
    return _to_response(file_profile, source="file")


@router.put("/", response_model=ProfileResponse)
def update_profile(
    body: ProfileUpdateRequest,
    repository: JobRepository = Depends(get_repository),
    current_user: dict = Depends(get_current_user),
):
    """Partial update to stored profile (merge-patch semantics)."""
    user_id = str(current_user.get("sub") or current_user.get("id", "anonymous"))
    stored = repository.get_profile_by_user(user_id)

    if stored and stored.get("profile_json"):
        try:
            existing = json.loads(stored["profile_json"])
        except Exception:
            existing = load_profile()
    else:
        existing = load_profile()

    update_fields = body.model_dump(exclude_none=True)
    existing.update(update_fields)

    existing = recompute_profile_skills(existing)
    existing["source"] = "manual"

    repository.upsert_profile(
        user_id=user_id,
        profile_json=json.dumps(existing),
        raw_text=stored.get("raw_text", "") if stored else "",
        source="manual",
        name=existing.get("name", ""),
        role=existing.get("role", ""),
        experience_level=existing.get("experience_level", ""),
    )

    return _to_response(existing, source="manual")
