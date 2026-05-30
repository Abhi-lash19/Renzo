import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from resume.parser import ParsedResume
from resume.builder import build_profile


def _make_parsed(**kwargs) -> ParsedResume:
    defaults = dict(
        name="Alice Smith",
        summary="Backend developer with Python expertise",
        raw_skills=["Python", "AWS", "Docker", "FastAPI", "PostgreSQL", "Figma"],
        experience_items=[
            "Software Engineer at TechCorp (2022-2024)",
            "Built REST APIs using Python and FastAPI",
            "Managed AWS Lambda deployments",
        ],
        project_items=["Built real-time analytics dashboard using Python and Kafka"],
        education_items=["B.Tech Computer Science, IIT Delhi (2018-2022)"],
        certification_items=[],
        detected_roles=["software engineer", "backend engineer"],
        extra_text="Bengaluru, India | alice@example.com",
    )
    defaults.update(kwargs)
    return ParsedResume(**defaults)


class TestBuildProfile:
    def test_returns_dict(self):
        assert isinstance(build_profile(_make_parsed()), dict)

    def test_has_all_required_pipeline_keys(self):
        result = build_profile(_make_parsed())
        required = [
            "core_skills", "secondary_skills", "all_skills", "weighted_skills",
            "preferred_roles", "target_roles", "exclude_keywords", "bonus_keywords",
            "preferred_keywords", "projects", "experience", "source",
        ]
        for key in required:
            assert key in result, f"Missing key: {key}"

    def test_core_skills_contains_python(self):
        result = build_profile(_make_parsed())
        core_lower = [s.lower() for s in result["core_skills"]]
        assert any("python" in s for s in core_lower)

    def test_all_skills_is_superset_of_core_and_secondary(self):
        result = build_profile(_make_parsed())
        all_set = set(result["all_skills"])
        assert set(result["core_skills"]).issubset(all_set)
        assert set(result["secondary_skills"]).issubset(all_set)

    def test_weighted_skills_has_correct_types(self):
        result = build_profile(_make_parsed())
        assert isinstance(result["weighted_skills"], dict)
        for k, v in result["weighted_skills"].items():
            assert isinstance(k, str), f"Key {k!r} is not a string"
            assert isinstance(v, float), f"Value for {k!r} is not float: {v!r}"

    def test_core_skills_have_weight_1_0(self):
        result = build_profile(_make_parsed())
        ws = result["weighted_skills"]
        for skill in result["core_skills"]:
            if skill in ws:
                assert ws[skill] == 1.0, f"Expected 1.0 for core skill {skill!r}, got {ws[skill]}"

    def test_secondary_skills_have_weight_0_6(self):
        result = build_profile(_make_parsed())
        ws = result["weighted_skills"]
        for skill in result["secondary_skills"]:
            if skill in ws and skill not in result["core_skills"]:
                assert ws[skill] <= 0.6 + 1e-9

    def test_projects_populated(self):
        result = build_profile(_make_parsed())
        assert len(result["projects"]) > 0

    def test_experience_populated(self):
        result = build_profile(_make_parsed())
        assert len(result["experience"]) > 0

    def test_preferred_roles_not_empty(self):
        result = build_profile(_make_parsed())
        assert len(result["preferred_roles"]) > 0

    def test_name_preserved(self):
        result = build_profile(_make_parsed(name="Bob Jones"))
        assert result.get("name") == "Bob Jones"

    def test_source_is_upload(self):
        result = build_profile(_make_parsed())
        assert result["source"] == "upload"

    def test_empty_parsed_resume_returns_valid_profile(self):
        empty = ParsedResume()
        result = build_profile(empty)
        assert isinstance(result, dict)
        assert "core_skills" in result
        assert isinstance(result["weighted_skills"], dict)

    def test_is_pipeline_compatible(self):
        """Profile must work as input to build_match_data() without modification."""
        result = build_profile(_make_parsed())
        from utils.profile_loader import load_profile
        file_profile = load_profile()
        for key in ["core_skills", "secondary_skills", "weighted_skills", "all_skills",
                    "target_roles", "exclude_keywords", "preferred_roles"]:
            assert key in result, f"Missing key: {key}"
            assert key in file_profile
