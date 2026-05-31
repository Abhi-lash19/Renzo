import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
import json
from evaluation.harness import EvaluationHarness
from evaluation.metrics import compute_all_metrics, ndcg_at_k, precision_at_k
from evaluation.datasets.golden_profiles import GOLDEN_PROFILES
from evaluation.datasets.golden_jobs import GOLDEN_JOBS
from evaluation.datasets.ground_truth import GROUND_TRUTH, get_relevant_job_ids, get_highly_relevant_job_ids


class TestGoldenDatasetIntegrity:
    def test_all_profiles_present(self):
        assert "junior-python-backend" in GOLDEN_PROFILES
        assert "aws-cloud-engineer" in GOLDEN_PROFILES
        assert "fullstack-developer" in GOLDEN_PROFILES

    def test_all_jobs_have_descriptions(self):
        for key, job in GOLDEN_JOBS.items():
            assert job.description and len(job.description) > 30, f"Job {key} has short description"

    def test_ground_truth_covers_all_jobs(self):
        all_job_ids = {f"eval_job_{k}" for k in GOLDEN_JOBS.keys()}
        for profile_id, labels in GROUND_TRUTH.items():
            labeled_ids = set(labels.keys())
            missing = all_job_ids - labeled_ids
            assert not missing, f"Profile {profile_id} missing labels: {missing}"

    def test_ground_truth_values_valid(self):
        for profile_id, labels in GROUND_TRUTH.items():
            for job_id, rel in labels.items():
                assert rel in (0, 1, 2), f"Invalid {rel} for {profile_id}/{job_id}"

    def test_each_profile_has_relevant_jobs(self):
        for profile_id in GOLDEN_PROFILES:
            relevant = get_relevant_job_ids(profile_id)
            assert len(relevant) >= 3, f"{profile_id} has only {len(relevant)} relevant"

    def test_each_profile_has_strongly_relevant_jobs(self):
        for profile_id in GOLDEN_PROFILES:
            strong = get_highly_relevant_job_ids(profile_id)
            assert len(strong) >= 2, f"{profile_id} has only {len(strong)} strong"

    def test_job_ids_are_unique(self):
        ids = [job.job_id for job in GOLDEN_JOBS.values()]
        assert len(ids) == len(set(ids))


class TestHarnessEndToEnd:
    def test_harness_produces_metrics_for_all_profiles(self):
        harness = EvaluationHarness()
        results = harness.evaluate_all()
        assert len(results) == 3
        for profile_id, result in results.items():
            assert result.metrics.precision_at_5 >= 0.0
            assert result.metrics.ndcg_at_10 >= 0.0
            assert result.metrics.mrr >= 0.0

    def test_relevant_jobs_rank_above_irrelevant(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["junior-python-backend"]
        python_job = GOLDEN_JOBS["j001"]  # Python/FastAPI — perfect match
        ios_job = GOLDEN_JOBS["j017"]     # iOS — excluded keyword
        ranked = harness.score_and_rank(profile, [python_job, ios_job])
        assert ranked[0].job_id == "eval_job_j001"

    def test_cloud_jobs_rank_above_frontend_for_cloud_profile(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["aws-cloud-engineer"]
        cloud_job = GOLDEN_JOBS["j009"]   # AWS Cloud Engineer — perfect match
        frontend_job = GOLDEN_JOBS["j016"]  # React Frontend — wrong domain
        ranked = harness.score_and_rank(profile, [cloud_job, frontend_job])
        assert ranked[0].job_id == "eval_job_j009"

    def test_fullstack_jobs_rank_for_fullstack_profile(self):
        harness = EvaluationHarness()
        profile = GOLDEN_PROFILES["fullstack-developer"]
        fullstack_job = GOLDEN_JOBS["j014"]  # Full Stack Python/React — perfect
        ios_job = GOLDEN_JOBS["j017"]         # iOS — excluded
        ranked = harness.score_and_rank(profile, [fullstack_job, ios_job])
        assert ranked[0].job_id == "eval_job_j014"

    def test_metrics_in_valid_range(self):
        harness = EvaluationHarness()
        results = harness.evaluate_all()
        for profile_id, result in results.items():
            for attr in ("precision_at_5", "precision_at_10", "recall_at_10", "ndcg_at_10", "mrr"):
                val = getattr(result.metrics, attr)
                assert 0.0 <= val <= 1.0, f"{profile_id}.{attr} = {val}"


class TestRunEvalIntegration:
    def test_run_evaluation_returns_structure(self):
        from evaluation.run_eval import run_evaluation
        results = run_evaluation()
        assert "per_profile" in results
        assert "aggregate" in results
        assert len(results["per_profile"]) == 3
        for key in ("mean_precision_at_5", "mean_ndcg_at_10", "mean_mrr"):
            assert key in results["aggregate"]

    def test_aggregate_metrics_in_range(self):
        from evaluation.run_eval import run_evaluation
        results = run_evaluation()
        for key, value in results["aggregate"].items():
            assert 0.0 <= value <= 1.0, f"{key} = {value} out of range"

    def test_save_and_load_baseline(self, tmp_path):
        from evaluation.run_eval import run_evaluation, save_baseline
        results = run_evaluation()
        output_path = tmp_path / "baseline.json"
        save_baseline(results, path=output_path)
        assert output_path.exists()
        loaded = json.loads(output_path.read_text())
        for key in ("timestamp", "per_profile", "aggregate", "metric_targets"):
            assert key in loaded


class TestMetricsMonotonicity:
    def test_ndcg_perfect_vs_imperfect(self):
        relevance = {"j1": 2, "j2": 2, "j3": 1, "j4": 0, "j5": 0}
        perfect = ["j1", "j2", "j3", "j4", "j5"]
        imperfect = ["j5", "j4", "j3", "j1", "j2"]
        assert ndcg_at_k(perfect, relevance, 5) >= ndcg_at_k(imperfect, relevance, 5)

    def test_precision_worse_with_irrelevant_at_top(self):
        relevance = {"j1": 2, "j2": 2, "j3": 0}
        good = ["j1", "j2", "j3"]
        bad = ["j3", "j1", "j2"]
        assert precision_at_k(good, relevance, 2) >= precision_at_k(bad, relevance, 2)

    def test_ndcg_bounded_0_to_1(self):
        import random
        random.seed(42)
        job_ids = [f"j{i}" for i in range(10)]
        relevance = {jid: random.randint(0, 2) for jid in job_ids}
        ranked = job_ids[:]
        random.shuffle(ranked)
        ndcg = ndcg_at_k(ranked, relevance, 10)
        assert 0.0 <= ndcg <= 1.0
