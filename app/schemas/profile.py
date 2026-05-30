from __future__ import annotations
from typing import Dict, List
from pydantic import BaseModel


class ProfileResponse(BaseModel):
    name: str = ""
    role: str = ""
    experience_level: str = "0-2 years"
    core_skills: List[str] = []
    secondary_skills: List[str] = []
    all_skills: List[str] = []
    weighted_skills: Dict[str, float] = {}
    cloud: List[str] = []
    devops: List[str] = []
    projects: List[str] = []
    experience: List[str] = []
    preferred_roles: List[str] = []
    target_roles: List[str] = []
    exclude_keywords: List[str] = []
    bonus_keywords: List[str] = []
    preferred_keywords: List[str] = []
    location: str = ""
    remote_preferred: bool = True
    source: str = "file"


class ProfileUpdateRequest(BaseModel):
    name: str | None = None
    role: str | None = None
    experience_level: str | None = None
    core_skills: List[str] | None = None
    secondary_skills: List[str] | None = None
    preferred_roles: List[str] | None = None
    target_roles: List[str] | None = None
    exclude_keywords: List[str] | None = None
    bonus_keywords: List[str] | None = None
    preferred_keywords: List[str] | None = None
    location: str | None = None
    remote_preferred: bool | None = None
