"""
resume/parser.py — Parse raw resume text into structured sections.

Uses rule-based section detection (keyword matching + heuristics).
No ML or AI — deterministic extraction layer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List

from utils.logger import get_logger

logger = get_logger(__name__)

SECTION_KEYWORDS: dict[str, list[str]] = {
    "summary": ["summary", "objective", "about", "profile", "overview", "introduction"],
    "skills": [
        "skills", "technical skills", "technologies", "tech stack", "tools",
        "competencies", "expertise", "technical expertise", "core competencies",
        "programming languages", "frameworks", "key skills", "technical",
    ],
    "experience": [
        "experience", "work experience", "employment", "professional experience",
        "work history", "career history", "career", "positions held",
    ],
    "education": [
        "education", "academic", "qualification", "degrees", "university",
        "college", "academic background", "academics",
    ],
    "projects": [
        "projects", "personal projects", "side projects", "portfolio",
        "open source", "github", "key projects", "notable projects",
    ],
    "certifications": [
        "certifications", "certificates", "awards", "achievements",
        "honors", "licenses", "accreditations",
    ],
}

JOB_TITLE_PATTERNS = re.compile(
    r"\b(software engineer|backend engineer|backend developer|python developer|"
    r"full[- ]?stack|senior engineer|junior engineer|associate engineer|"
    r"devops engineer|cloud engineer|sre|site reliability|data engineer|"
    r"machine learning|ml engineer|tech lead|engineering manager|intern)\b",
    re.IGNORECASE,
)

_SKILL_SPLIT_RE = re.compile(r"[,;|•\n]")
_HEADER_RE = re.compile(r"^[A-Z][A-Z\s/&\-]{2,}:?\s*$")


@dataclass
class ParsedResume:
    """Structured data extracted from raw resume text."""
    name: str = ""
    summary: str = ""
    raw_skills: List[str] = field(default_factory=list)
    experience_items: List[str] = field(default_factory=list)
    project_items: List[str] = field(default_factory=list)
    education_items: List[str] = field(default_factory=list)
    certification_items: List[str] = field(default_factory=list)
    detected_roles: List[str] = field(default_factory=list)
    extra_text: str = ""


def _classify_line_as_section(line: str) -> str | None:
    """Return the canonical section name if line is a section header, else None.

    Only matches lines that are plausibly section headers: either an exact keyword
    match, or a short line (<=40 chars) whose content matches a keyword closely.
    This prevents body text lines like "2 years of experience building..." from
    being misclassified as section headers.
    """
    cleaned = line.strip().rstrip(":").lower()
    if not cleaned:
        return None
    # Exact match always wins
    for section, keywords in SECTION_KEYWORDS.items():
        if cleaned in keywords:
            return section
    # For fuzzy matching, only consider short lines that look like headers
    if len(cleaned) <= 40:
        for section, keywords in SECTION_KEYWORDS.items():
            if any(kw in cleaned for kw in keywords if len(kw) >= 5):
                return section
    return None


def _is_header_line(line: str) -> bool:
    """True if line looks like a section header (ALL CAPS or short title-case ending with colon)."""
    stripped = line.strip()
    if not stripped:
        return False
    if _HEADER_RE.match(stripped):
        return True
    if stripped.endswith(":") and stripped[:-1].istitle() and len(stripped) < 60:
        return True
    return False


def _extract_skills_from_text(text: str) -> List[str]:
    """Split skill-section text into individual skill tokens."""
    parts = _SKILL_SPLIT_RE.split(text)
    skills = []
    for part in parts:
        cleaned = re.sub(r"^[\s•\-→·▪▸*]+|[\s.]+$", "", part.strip())
        if cleaned and 2 <= len(cleaned) <= 60:
            skills.append(cleaned)
    return skills


def _extract_roles_from_text(text: str) -> List[str]:
    """Extract job titles from experience section text."""
    matches = JOB_TITLE_PATTERNS.findall(text)
    return list(dict.fromkeys(m.strip().lower() for m in matches if m.strip()))


def _extract_name_heuristic(lines: List[str]) -> str:
    """Heuristically extract the candidate's name from the top of the resume."""
    contact_pattern = re.compile(r"[@+\(\)\d\/\.]")
    for line in lines[:8]:
        stripped = line.strip()
        if (
            stripped
            and len(stripped) <= 60
            and not contact_pattern.search(stripped)
            and not stripped.startswith("#")
            and not _classify_line_as_section(stripped)
            and any(c.isupper() for c in stripped)
        ):
            return stripped
    return ""


def parse_resume(text: str) -> ParsedResume:
    """
    Parse raw resume text into structured sections.

    Args:
        text: Raw extracted resume text.

    Returns:
        ParsedResume dataclass with populated fields.
    """
    result = ParsedResume()
    if not text or not text.strip():
        return result

    lines = text.splitlines()
    result.name = _extract_name_heuristic(lines)

    current_section: str | None = None
    section_buffers: dict[str, list[str]] = {
        "summary": [], "skills": [], "experience": [],
        "education": [], "projects": [], "certifications": [], "extra": [],
    }

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        section = _classify_line_as_section(stripped)
        if section:
            current_section = section
            # Also handle inline content after the header (e.g. "Skills: Python, AWS")
            if ":" in stripped:
                _, _, inline_content = stripped.partition(":")
                if inline_content.strip():
                    section_buffers[section].append(inline_content.strip())
            continue

        if _is_header_line(stripped):
            detected = _classify_line_as_section(stripped)
            if detected:
                current_section = detected
            continue

        target = current_section if current_section else "extra"
        if target in section_buffers:
            section_buffers[target].append(stripped)

    # Post-process
    skills_text = "\n".join(section_buffers["skills"])
    result.raw_skills = _extract_skills_from_text(skills_text)
    result.experience_items = [l for l in section_buffers["experience"] if len(l) >= 5]
    result.detected_roles = _extract_roles_from_text("\n".join(section_buffers["experience"]))
    result.project_items = [l for l in section_buffers["projects"] if len(l) >= 5]
    result.education_items = [l for l in section_buffers["education"] if len(l) >= 5]
    result.certification_items = section_buffers["certifications"]
    result.summary = " ".join(section_buffers["summary"])
    result.extra_text = "\n".join(section_buffers["extra"])

    logger.debug(
        f"[PARSER] name='{result.name}' skills={len(result.raw_skills)} "
        f"experience={len(result.experience_items)} projects={len(result.project_items)}"
    )
    return result
