"""
evaluation/harness.py — Evaluation harness for running the scoring pipeline against golden data.

EvaluationHarness runs the REAL production pipeline (build_match_data -> score_job -> classify_job)
against synthetic golden job data. No mocking — this measures actual system behavior.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Dict, List, TYPE_CHECKING

from evaluation.metrics import MetricsResult, compute_all_metrics
from utils.logger import get_logger

if TYPE_CHECKING:
    from pipeline.models import Job

logger = get_logger(__name__)


@dataclass
class EvalResult:
    """Evaluation result for a single profile."""
    profile_id: str
    ranked_ids: List[str] = field(default_factory=list)
    ranked_scores: List[float] = field(default_factory=list)
    match_types: Dict[str, str] = field(default_factory=dict)
    metrics: MetricsResult = field(default_factory=MetricsResult)

    def to_dict(self) -> dict:
        return {
            "profile_id": self.profile_id,
            "ranked_ids": self.ranked_ids[:10],
            "ranked_scores": [round(s, 4) for s in self.ranked_scores[:10]],
            "match_types": {k: v for k, v in list(self.match_types.items())[:10]},
            "metrics": self.metrics.to_dict(),
        }


class EvaluationHarness:
    """
    Runs the production scoring pipeline against golden datasets and computes retrieval metrics.

    Uses the real build_match_data(), score_job(), and classify_job() — no mocking.
    """

    def score_and_rank(
        self,
        profile: dict,
        jobs: "List[Job]",
    ) -> "List[Job]":
        """
        Score all jobs against the profile and return them sorted by score descending.

        Works on shallow copies to avoid mutating the golden dataset objects.
        """
        from utils.matching_engine import build_match_data
        from pipeline.scorer import score_job
        from pipeline.classifier import classify_job

        if not jobs:
            return []

        scored_jobs = []
        for job in jobs:
            try:
                job_copy = copy.copy(job)
                build_match_data(job_copy, profile)
                score_job(job_copy, profile)
                transferable_count = len(
                    getattr(job_copy, "match_data", {}).get("transferable_skills", [])
                )
                job_copy.match_type = classify_job(
                    score=job_copy.score, transferable_count=transferable_count
                )
                scored_jobs.append(job_copy)
            except Exception as e:
                logger.warning(f"[HARNESS] Scoring failed for {job.job_id}: {e}")

        scored_jobs.sort(key=lambda j: j.score, reverse=True)
        return scored_jobs

    def evaluate_single(
        self,
        profile: dict,
        jobs: "List[Job]",
        graded_relevance: Dict[str, int],
    ) -> EvalResult:
        """
        Score and rank all jobs, then compute metrics against ground truth.

        Args:
            profile:          Profile dict (same format as load_profile() output).
            jobs:             List of Job objects to score.
            graded_relevance: Ground truth {job_id: 0/1/2}.

        Returns:
            EvalResult with ranked job IDs, scores, match types, and metrics.
        """
        profile_id = profile.get("name", "unknown")
        ranked = self.score_and_rank(profile, jobs)
        ranked_ids = [j.job_id for j in ranked]
        ranked_scores = [j.score for j in ranked]
        match_types = {j.job_id: getattr(j, "match_type", "") for j in ranked}
        metrics = compute_all_metrics(ranked_ids, graded_relevance)

        logger.info(
            f"[HARNESS] '{profile_id}': "
            f"P@5={metrics.precision_at_5:.3f} "
            f"P@10={metrics.precision_at_10:.3f} "
            f"NDCG@10={metrics.ndcg_at_10:.3f} "
            f"MRR={metrics.mrr:.3f}"
        )

        return EvalResult(
            profile_id=profile_id,
            ranked_ids=ranked_ids,
            ranked_scores=ranked_scores,
            match_types=match_types,
            metrics=metrics,
        )

    def evaluate_all(self) -> Dict[str, EvalResult]:
        """Run evaluation for all golden profiles against all golden jobs."""
        from evaluation.datasets.golden_profiles import GOLDEN_PROFILES
        from evaluation.datasets.golden_jobs import GOLDEN_JOBS
        from evaluation.datasets.ground_truth import GROUND_TRUTH

        all_jobs = list(GOLDEN_JOBS.values())
        results: Dict[str, EvalResult] = {}

        for profile_id, profile in GOLDEN_PROFILES.items():
            relevance = GROUND_TRUTH.get(profile_id, {})
            result = self.evaluate_single(profile, all_jobs, relevance)
            result.profile_id = profile_id
            results[profile_id] = result

        return results
