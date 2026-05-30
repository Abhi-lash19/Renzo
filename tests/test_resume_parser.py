import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from resume.parser import ParsedResume, parse_resume


SAMPLE_RESUME = """
John Doe
john@example.com | +91-9876543210 | Bengaluru, India | linkedin.com/in/johndoe

SUMMARY
Backend developer with 2 years of experience building scalable Python APIs on AWS.

TECHNICAL SKILLS
Python, FastAPI, AWS Lambda, Docker, PostgreSQL, Redis, Kubernetes, Terraform, REST APIs, Microservices

WORK EXPERIENCE
Software Engineer — TechCorp (Jan 2023 – Present)
- Built event-driven microservices using Python and AWS SQS
- Designed REST APIs serving 100K+ daily requests using FastAPI
- Automated CI/CD pipelines using GitHub Actions and Terraform

EDUCATION
B.Tech Computer Science — IIT Delhi (2019–2023)

PROJECTS
Real-time Analytics Dashboard
- Built a real-time analytics system using Python, Kafka, and PostgreSQL
- Deployed on AWS ECS with Docker
"""


class TestParseResume:
    def test_returns_parsed_resume_instance(self):
        result = parse_resume(SAMPLE_RESUME)
        assert isinstance(result, ParsedResume)

    def test_extracts_skills_from_skills_section(self):
        result = parse_resume(SAMPLE_RESUME)
        skills_lower = [s.lower() for s in result.raw_skills]
        assert any("python" in s for s in skills_lower)
        assert any("docker" in s for s in skills_lower)

    def test_extracts_multiple_skills(self):
        result = parse_resume(SAMPLE_RESUME)
        assert len(result.raw_skills) >= 5

    def test_detects_experience_section(self):
        result = parse_resume(SAMPLE_RESUME)
        assert len(result.experience_items) > 0

    def test_detects_project_section(self):
        result = parse_resume(SAMPLE_RESUME)
        assert len(result.project_items) > 0

    def test_detects_education(self):
        result = parse_resume(SAMPLE_RESUME)
        assert len(result.education_items) > 0

    def test_detects_summary(self):
        result = parse_resume(SAMPLE_RESUME)
        assert len(result.summary) > 0
        assert "backend" in result.summary.lower() or "developer" in result.summary.lower()

    def test_empty_text_returns_empty_parsed_resume(self):
        result = parse_resume("")
        assert isinstance(result, ParsedResume)
        assert result.raw_skills == []
        assert result.experience_items == []

    def test_name_is_string(self):
        result = parse_resume(SAMPLE_RESUME)
        assert isinstance(result.name, str)

    def test_skills_section_with_pipe_separator(self):
        text = "SKILLS\nPython | JavaScript | React | AWS | Docker"
        result = parse_resume(text)
        skills_lower = [s.lower() for s in result.raw_skills]
        assert any("python" in s for s in skills_lower)

    def test_skills_section_with_bullets(self):
        text = "Technical Skills:\n• Python\n• Docker\n• Kubernetes\n• PostgreSQL"
        result = parse_resume(text)
        assert len(result.raw_skills) >= 3

    def test_resume_with_only_skills(self):
        text = "Skills: Python, AWS, FastAPI, PostgreSQL"
        result = parse_resume(text)
        assert len(result.raw_skills) >= 3

    def test_detected_roles_from_experience(self):
        result = parse_resume(SAMPLE_RESUME)
        assert isinstance(result.detected_roles, list)

    def test_parsed_resume_is_dataclass(self):
        from dataclasses import is_dataclass
        assert is_dataclass(ParsedResume)
