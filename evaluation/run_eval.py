"""
evaluation/run_eval.py — Evaluation runner and baseline recorder.

Usage:
  As library:  from evaluation.run_eval import run_evaluation
  As CLI:      python -m evaluation.run_eval
  Via main:    python main.py --evaluate
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from evaluation.harness import EvalResult, EvaluationHarness
from utils.logger import get_logger

logger = get_logger(__name__)

RESULTS_DIR = Path(__file__).resolve().parent / "results"
BASELINE_PATH = RESULTS_DIR / "baseline.json"

# Targets — Phase 8 (AI) must improve over these numbers
METRIC_TARGETS = {
    "precision_at_5": 0.80,
    "precision_at_10": 0.75,
    "recall_at_10": 0.70,
    "ndcg_at_10": 0.70,
    "mrr": 0.80,
}


def run_evaluation() -> Dict[str, dict]:
    """
    Run the evaluation harness on all golden profiles and return aggregated results.

    Returns:
        Dict with "per_profile" (per-profile metric dicts) and "aggregate" (means).
    """
    harness = EvaluationHarness()
    eval_results: Dict[str, EvalResult] = harness.evaluate_all()

    per_profile = {
        profile_id: result.to_dict()
        for profile_id, result in eval_results.items()
    }

    metric_keys = ["precision_at_5", "precision_at_10", "recall_at_10", "ndcg_at_10", "ndcg_at_5", "mrr"]
    aggregate: Dict[str, float] = {}
    n_profiles = len(per_profile)
    for key in metric_keys:
        if n_profiles > 0:
            total = sum(
                per_profile[pid]["metrics"].get(key, 0.0)
                for pid in per_profile
            )
            aggregate[f"mean_{key}"] = round(total / n_profiles, 4)

    return {"per_profile": per_profile, "aggregate": aggregate}


def save_baseline(results: dict, path: Path = BASELINE_PATH) -> None:
    """Save evaluation results to a JSON baseline file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system_version": "Phase 7 baseline — keyword scoring + transferable skill adjacency",
        "metric_targets": METRIC_TARGETS,
        **results,
    }
    path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    logger.info(f"[EVAL] Baseline saved to {path}")


def print_results(results: dict) -> None:
    """Print a formatted evaluation summary to stdout."""
    print("\n" + "=" * 60)
    print("RENZO EVALUATION RESULTS")
    print("=" * 60)

    for profile_id, data in results["per_profile"].items():
        metrics = data["metrics"]
        print(f"\nProfile: {profile_id}")
        top5 = data.get("ranked_ids", [])[:5]
        print(f"  Top-5 ranked: {top5}")
        for metric_key, target in METRIC_TARGETS.items():
            val = metrics.get(metric_key, 0.0)
            status = "✓" if val >= target else "✗"
            print(f"  {metric_key}: {val:.3f} {status} (target: >= {target:.2f})")

    print("\n" + "-" * 60)
    print("AGGREGATE (mean across all profiles)")
    for key, value in results["aggregate"].items():
        metric_name = key.replace("mean_", "")
        target = METRIC_TARGETS.get(metric_name)
        target_str = f"(target: >= {target:.2f})" if target else ""
        status = "✓" if (target and value >= target) else ("✗" if target else "")
        print(f"  {key}: {value:.3f} {status} {target_str}")
    print("=" * 60)


if __name__ == "__main__":
    print("Running Renzo evaluation framework...")
    results = run_evaluation()
    print_results(results)
    save_baseline(results)
    print(f"\nBaseline saved to: {BASELINE_PATH}")
    sys.exit(0)
