import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from intelligence.skill_adjacency import (
    SKILL_GRAPH,
    get_adjacent_skills,
    get_transferable_skills,
)


class TestSkillGraphConstants:
    def test_skill_graph_is_dict(self):
        assert isinstance(SKILL_GRAPH, dict)

    def test_skill_graph_has_at_least_50_skills(self):
        all_skills = set(SKILL_GRAPH.keys())
        assert len(all_skills) >= 50, f"Only {len(all_skills)} skills in graph"

    def test_all_confidence_values_in_range(self):
        for skill, adjacencies in SKILL_GRAPH.items():
            for adjacent_skill, confidence in adjacencies.items():
                assert 0.0 < confidence <= 1.0, (
                    f"Out-of-range: {skill}→{adjacent_skill}: {confidence}"
                )

    def test_all_keys_are_lowercase(self):
        for skill in SKILL_GRAPH:
            assert skill == skill.lower(), f"Non-lowercase key: {skill!r}"

    def test_all_adjacent_skills_lowercase(self):
        for skill, adj in SKILL_GRAPH.items():
            for adj_skill in adj:
                assert adj_skill == adj_skill.lower(), f"Non-lowercase adjacent in {skill}: {adj_skill!r}"

    def test_no_self_loops(self):
        for skill, adj in SKILL_GRAPH.items():
            assert skill not in adj, f"Self-loop: {skill}→{skill}"

    def test_fastapi_flask_adjacency_exists(self):
        assert "fastapi" in SKILL_GRAPH
        assert "flask" in SKILL_GRAPH.get("fastapi", {})

    def test_aws_gcp_adjacency_exists(self):
        assert "aws" in SKILL_GRAPH
        assert "gcp" in SKILL_GRAPH.get("aws", {})

    def test_postgresql_mysql_adjacency_exists(self):
        assert "postgresql" in SKILL_GRAPH
        assert "mysql" in SKILL_GRAPH.get("postgresql", {})

    def test_docker_is_in_graph(self):
        assert "docker" in SKILL_GRAPH

    def test_kubernetes_eks_adjacency_exists(self):
        assert "kubernetes" in SKILL_GRAPH
        assert "eks" in SKILL_GRAPH.get("kubernetes", {})


class TestGetAdjacentSkills:
    def test_returns_list(self):
        assert isinstance(get_adjacent_skills("fastapi"), list)

    def test_known_skill_returns_nonempty(self):
        assert len(get_adjacent_skills("fastapi")) > 0

    def test_unknown_skill_returns_empty(self):
        assert get_adjacent_skills("nonexistent_skill_xyz_999") == []

    def test_results_are_tuples_of_two(self):
        for item in get_adjacent_skills("fastapi"):
            assert isinstance(item, tuple) and len(item) == 2

    def test_sorted_by_confidence_descending(self):
        result = get_adjacent_skills("aws")
        confidences = [c for _, c in result]
        assert confidences == sorted(confidences, reverse=True)

    def test_min_confidence_filter(self):
        result = get_adjacent_skills("fastapi", min_confidence=0.8)
        assert all(c >= 0.8 for _, c in result)

    def test_flask_adjacent_to_fastapi(self):
        adj = dict(get_adjacent_skills("fastapi"))
        assert "flask" in adj and adj["flask"] >= 0.7

    def test_empty_string_returns_empty(self):
        assert get_adjacent_skills("") == []


class TestGetTransferableSkills:
    def test_returns_list(self):
        assert isinstance(get_transferable_skills(["fastapi"], ["flask"]), list)

    def test_direct_adjacency_found(self):
        result = get_transferable_skills(["fastapi"], ["flask"])
        assert len(result) >= 1
        ps, js, conf = result[0]
        assert ps == "fastapi" and js == "flask" and conf >= 0.5

    def test_no_adjacency_returns_empty(self):
        assert get_transferable_skills(["kubernetes"], ["pandas"]) == []

    def test_empty_profile_returns_empty(self):
        assert get_transferable_skills([], ["python"]) == []

    def test_empty_job_skills_returns_empty(self):
        assert get_transferable_skills(["python"], []) == []

    def test_both_empty_returns_empty(self):
        assert get_transferable_skills([], []) == []

    def test_result_tuples_have_three_elements(self):
        result = get_transferable_skills(["aws"], ["gcp"])
        for item in result:
            assert len(item) == 3

    def test_confidence_meets_minimum(self):
        result = get_transferable_skills(["fastapi"], ["flask", "django"])
        assert all(conf >= 0.5 for _, _, conf in result)

    def test_min_confidence_filters(self):
        high = get_transferable_skills(["python"], ["golang"], min_confidence=0.9)
        low = get_transferable_skills(["python"], ["golang"], min_confidence=0.1)
        assert len(high) <= len(low)

    def test_multiple_profile_and_job_skills(self):
        result = get_transferable_skills(["fastapi", "aws"], ["flask", "gcp"])
        pairs = [(ps, js) for ps, js, _ in result]
        assert ("fastapi", "flask") in pairs
        assert ("aws", "gcp") in pairs

    def test_no_duplicate_pairs(self):
        result = get_transferable_skills(["fastapi"], ["flask"])
        pairs = [(ps, js) for ps, js, _ in result]
        assert len(pairs) == len(set(pairs))

    def test_results_are_deterministic(self):
        a = get_transferable_skills(["fastapi", "aws"], ["flask", "gcp", "django"])
        b = get_transferable_skills(["fastapi", "aws"], ["flask", "gcp", "django"])
        assert a == b

    def test_kubernetes_ecs_transferable(self):
        result = get_transferable_skills(["kubernetes"], ["ecs"])
        assert len(result) >= 1
