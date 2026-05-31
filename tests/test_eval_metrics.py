import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from evaluation.metrics import (
    precision_at_k,
    recall_at_k,
    ndcg_at_k,
    mrr,
    compute_all_metrics,
    MetricsResult,
)


class TestPrecisionAtK:
    def test_all_relevant_is_1(self):
        ranked = ["j1", "j2", "j3"]
        relevance = {"j1": 2, "j2": 1, "j3": 2}
        assert precision_at_k(ranked, relevance, k=3) == 1.0

    def test_none_relevant_is_0(self):
        ranked = ["j4", "j5", "j6"]
        relevance = {"j4": 0, "j5": 0, "j6": 0}
        assert precision_at_k(ranked, relevance, k=3) == 0.0

    def test_half_relevant(self):
        ranked = ["j1", "j4", "j2", "j5"]
        relevance = {"j1": 2, "j2": 1, "j4": 0, "j5": 0}
        assert abs(precision_at_k(ranked, relevance, k=4) - 0.5) < 1e-6

    def test_k_larger_than_ranked(self):
        ranked = ["j1", "j2"]
        relevance = {"j1": 1, "j2": 0}
        # 2 items, k=5 → denominator is k=5, numerator is 1 → 0.2
        assert abs(precision_at_k(ranked, relevance, k=5) - 0.2) < 1e-6

    def test_truncates_to_k(self):
        ranked = ["j1", "j2", "j3", "j4", "j5"]
        relevance = {"j1": 0, "j2": 0, "j3": 2, "j4": 2, "j5": 2}
        assert precision_at_k(ranked, relevance, k=2) == 0.0

    def test_empty_ranked_is_0(self):
        assert precision_at_k([], {"j1": 2}, k=5) == 0.0

    def test_missing_relevance_treated_as_0(self):
        ranked = ["j1", "unknown"]
        relevance = {"j1": 2}
        assert abs(precision_at_k(ranked, relevance, k=2) - 0.5) < 1e-6


class TestRecallAtK:
    def test_all_relevant_retrieved(self):
        ranked = ["j1", "j2", "j3"]
        relevance = {"j1": 2, "j2": 1, "j3": 2, "j4": 0}
        assert recall_at_k(ranked, relevance, k=3) == 1.0

    def test_none_retrieved(self):
        ranked = ["j4", "j5"]
        relevance = {"j1": 2, "j2": 1, "j4": 0, "j5": 0}
        assert recall_at_k(ranked, relevance, k=2) == 0.0

    def test_partial_recall(self):
        ranked = ["j1", "j4", "j2", "j5"]
        relevance = {"j1": 2, "j2": 1, "j3": 2, "j4": 0, "j5": 0}
        assert abs(recall_at_k(ranked, relevance, k=4) - 2.0/3.0) < 1e-6

    def test_no_relevant_items_returns_0(self):
        ranked = ["j1", "j2"]
        relevance = {"j1": 0, "j2": 0}
        assert recall_at_k(ranked, relevance, k=2) == 0.0

    def test_empty_ranked_returns_0(self):
        assert recall_at_k([], {"j1": 2}, k=5) == 0.0


class TestNDCGAtK:
    def test_perfect_ranking_is_1(self):
        ranked = ["j1", "j2", "j3"]
        relevance = {"j1": 2, "j2": 1, "j3": 0}
        assert abs(ndcg_at_k(ranked, relevance, k=3) - 1.0) < 1e-6

    def test_reversed_ranking_less_than_1(self):
        ranked = ["j3", "j2", "j1"]
        relevance = {"j1": 2, "j2": 1, "j3": 0}
        result = ndcg_at_k(ranked, relevance, k=3)
        assert result < 1.0 and result >= 0.0

    def test_no_relevant_returns_0(self):
        ranked = ["j1", "j2"]
        relevance = {"j1": 0, "j2": 0}
        assert ndcg_at_k(ranked, relevance, k=2) == 0.0

    def test_result_in_0_to_1_range(self):
        ranked = ["j1", "j3", "j2", "j4"]
        relevance = {"j1": 2, "j2": 2, "j3": 0, "j4": 1}
        result = ndcg_at_k(ranked, relevance, k=4)
        assert 0.0 <= result <= 1.0

    def test_empty_ranked_returns_0(self):
        assert ndcg_at_k([], {"j1": 2}, k=5) == 0.0

    def test_k_equals_1_perfect(self):
        ranked = ["j1", "j3"]
        relevance = {"j1": 2, "j3": 0}
        assert abs(ndcg_at_k(ranked, relevance, k=1) - 1.0) < 1e-6


class TestMRR:
    def test_first_is_relevant(self):
        ranked = ["j1", "j2", "j3"]
        relevance = {"j1": 2, "j2": 0, "j3": 0}
        assert abs(mrr(ranked, relevance) - 1.0) < 1e-6

    def test_second_is_first_relevant(self):
        ranked = ["j4", "j1", "j2"]
        relevance = {"j1": 2, "j2": 1, "j4": 0}
        assert abs(mrr(ranked, relevance) - 0.5) < 1e-6

    def test_no_relevant_returns_0(self):
        ranked = ["j1", "j2"]
        relevance = {"j1": 0, "j2": 0}
        assert mrr(ranked, relevance) == 0.0

    def test_empty_returns_0(self):
        assert mrr([], {"j1": 2}) == 0.0


class TestComputeAllMetrics:
    def test_returns_metrics_result(self):
        ranked = ["j1", "j2", "j3", "j4", "j5"]
        relevance = {"j1": 2, "j2": 1, "j3": 0, "j4": 2, "j5": 0}
        result = compute_all_metrics(ranked, relevance)
        assert isinstance(result, MetricsResult)

    def test_has_standard_metric_attrs(self):
        ranked = ["j1", "j2"]
        relevance = {"j1": 2, "j2": 0}
        result = compute_all_metrics(ranked, relevance)
        for attr in ("precision_at_5", "precision_at_10", "recall_at_10", "ndcg_at_10", "mrr"):
            assert hasattr(result, attr)

    def test_all_metrics_in_0_to_1_range(self):
        ranked = ["j1", "j2", "j3", "j4", "j5"]
        relevance = {"j1": 2, "j2": 1, "j3": 0, "j4": 2, "j5": 0}
        result = compute_all_metrics(ranked, relevance)
        for attr in ("precision_at_5", "precision_at_10", "recall_at_10", "ndcg_at_10", "mrr"):
            val = getattr(result, attr)
            assert 0.0 <= val <= 1.0, f"{attr} = {val}"

    def test_to_dict_returns_dict(self):
        ranked = ["j1"]
        relevance = {"j1": 2}
        result = compute_all_metrics(ranked, relevance)
        d = result.to_dict()
        assert isinstance(d, dict)
        assert "precision_at_5" in d
