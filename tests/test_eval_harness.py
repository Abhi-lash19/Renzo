import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from evaluation.harness import EvaluationHarness, EvalResult
from evaluation.datasets.golden_profiles import GOLDEN_PROFILES
from evaluation.datasets.golden_jobs import GOLDEN_JOBS
from evaluation.datasets.ground_truth import GROUND_TRUTH


class TestEvaluationHarness:
    def test_harness_instantiates(self):
        assert EvaluationHarness() is not None

    def test_score_and_rank_returns_sorted_list(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        jobs = list(GOLDEN_JOBS.values())[:5]
        ranked = harness.score_and_rank(profile, jobs)
        assert isinstance(ranked, list)
        if len(ranked) >= 2:
            assert ranked[0].score >= ranked[1].score

    def test_score_and_rank_returns_all_jobs(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        jobs = list(GOLDEN_JOBS.values())[:5]
        ranked = harness.score_and_rank(profile, jobs)
        assert len(ranked) == 5

    def test_score_and_rank_empty_jobs(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        assert harness.score_and_rank(profile, []) == []

    def test_evaluate_single_returns_eval_result(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        jobs = list(GOLDEN_JOBS.values())
        relevance = GROUND_TRUTH["junior-python-backend"]
        result = harness.evaluate_single(profile, jobs, relevance)
        assert isinstance(result, EvalResult)

    def test_eval_result_has_metrics(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        jobs = list(GOLDEN_JOBS.values())
        relevance = GROUND_TRUTH["junior-python-backend"]
        result = harness.evaluate_single(profile, jobs, relevance)
        from evaluation.metrics import MetricsResult
        assert isinstance(result.metrics, MetricsResult)

    def test_eval_result_has_ranked_ids(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        jobs = list(GOLDEN_JOBS.values())
        relevance = GROUND_TRUTH["junior-python-backend"]
        result = harness.evaluate_single(profile, jobs, relevance)
        assert isinstance(result.ranked_ids, list)
        assert len(result.ranked_ids) == len(jobs)

    def test_evaluate_all_returns_all_profiles(self):
        harness = EvaluationHarness()
        results = harness.evaluate_all()
        assert isinstance(results, dict)
        assert "junior-python-backend" in results
        assert "aws-cloud-engineer" in results
        assert "fullstack-developer" in results

    def test_relevant_jobs_score_higher_than_irrelevant(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        # j001 = Python/FastAPI (perfect match), j017 = iOS (excluded)
        python_job = GOLDEN_JOBS["j001"]
        ios_job = GOLDEN_JOBS["j017"]
        ranked = harness.score_and_rank(profile, [python_job, ios_job])
        assert ranked[0].job_id == "eval_job_j001", (
            f"Expected Python job first, got {ranked[0].job_id}. "
            f"Scores: python={python_job.score:.2f}, ios={ios_job.score:.2f}"
        )
