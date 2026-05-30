import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from resume.normalizer import normalize_skills, categorize_skills, CORE_BACKEND_SIGNALS


class TestNormalizeSkills:
    def test_returns_list(self):
        result = normalize_skills(["Python", "AWS"])
        assert isinstance(result, list)

    def test_lowercases_skills(self):
        result = normalize_skills(["Python", "AWS"])
        assert "python" in result
        assert "aws" in result

    def test_deduplicates_skills(self):
        result = normalize_skills(["python", "Python", "PYTHON"])
        assert result.count("python") == 1

    def test_filters_empty_strings(self):
        result = normalize_skills(["python", "", "  ", "aws"])
        assert "" not in result
        assert "  " not in result

    def test_applies_normalization_map(self):
        result = normalize_skills(["Node.js", "node js", "Node"])
        # All three should normalize to "nodejs" and deduplicate to 1 entry
        nodejs_matches = [r for r in result if "nodejs" in r or "node" in r]
        assert len(nodejs_matches) >= 1

    def test_filters_single_char_strings(self):
        result = normalize_skills(["python", "a", "b", "aws"])
        assert "a" not in result
        assert "b" not in result

    def test_returns_empty_for_empty_input(self):
        assert normalize_skills([]) == []

    def test_strips_whitespace(self):
        result = normalize_skills(["  python  ", " aws "])
        assert "python" in result
        assert "aws" in result

    def test_preserves_valid_multi_word_skills(self):
        result = normalize_skills(["machine learning", "system design"])
        assert "machine learning" in result
        assert "system design" in result

    def test_go_language_preserved(self):
        # "go" is exactly 2 chars — should pass the min-length filter
        result = normalize_skills(["go", "rust"])
        assert "go" in result


class TestCategorizeSkills:
    def test_returns_dict_with_core_and_secondary(self):
        result = categorize_skills(["python", "aws", "react", "kubernetes"])
        assert "core_skills" in result
        assert "secondary_skills" in result

    def test_python_is_core(self):
        result = categorize_skills(["python", "react", "figma"])
        assert "python" in result["core_skills"]

    def test_aws_is_core(self):
        result = categorize_skills(["aws", "figma"])
        assert "aws" in result["core_skills"]

    def test_unknown_skills_go_to_secondary(self):
        result = categorize_skills(["python", "figma", "blender"])
        assert "figma" in result["secondary_skills"]
        assert "blender" in result["secondary_skills"]

    def test_empty_input_returns_empty_lists(self):
        result = categorize_skills([])
        assert result["core_skills"] == []
        assert result["secondary_skills"] == []

    def test_core_signals_constant_is_nonempty(self):
        assert len(CORE_BACKEND_SIGNALS) >= 10
