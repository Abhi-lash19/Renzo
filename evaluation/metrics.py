"""
evaluation/metrics.py — Retrieval quality metrics for the evaluation framework.

All functions are pure (no I/O, no side effects).
- ranked_ids:       list of job_id strings in ranked order (best first)
- graded_relevance: dict mapping job_id → relevance (0=not relevant, 1=borderline, 2=highly relevant)
- k:                cutoff position

Relevance > 0 counts as "relevant" for binary metrics (Precision, Recall, MRR).
Graded relevance (0/1/2) is used for NDCG.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class MetricsResult:
    """Computed retrieval quality metrics for a single profile evaluation."""
    precision_at_5: float = 0.0
    precision_at_10: float = 0.0
    recall_at_10: float = 0.0
    ndcg_at_5: float = 0.0
    ndcg_at_10: float = 0.0
    mrr: float = 0.0
    total_relevant: int = 0
    ranked_count: int = 0

    def to_dict(self) -> dict:
        return {
            "precision_at_5": round(self.precision_at_5, 4),
            "precision_at_10": round(self.precision_at_10, 4),
            "recall_at_10": round(self.recall_at_10, 4),
            "ndcg_at_5": round(self.ndcg_at_5, 4),
            "ndcg_at_10": round(self.ndcg_at_10, 4),
            "mrr": round(self.mrr, 4),
            "total_relevant": self.total_relevant,
            "ranked_count": self.ranked_count,
        }


def precision_at_k(
    ranked_ids: List[str],
    graded_relevance: Dict[str, int],
    k: int,
) -> float:
    """
    Fraction of top-K results that are relevant (relevance > 0).

    Denominator is always K, even if fewer items are ranked.
    Missing items (not in ranked_ids[:k]) are treated as not retrieved.
    """
    if not ranked_ids or k <= 0:
        return 0.0
    top_k = ranked_ids[:k]
    relevant_in_top_k = sum(1 for jid in top_k if graded_relevance.get(jid, 0) > 0)
    return relevant_in_top_k / k


def recall_at_k(
    ranked_ids: List[str],
    graded_relevance: Dict[str, int],
    k: int,
) -> float:
    """
    Fraction of all relevant items that appear in the top-K results.

    Returns 0.0 if there are no relevant items in the ground truth.
    """
    if not ranked_ids or k <= 0:
        return 0.0
    total_relevant = sum(1 for rel in graded_relevance.values() if rel > 0)
    if total_relevant == 0:
        return 0.0
    top_k = ranked_ids[:k]
    relevant_in_top_k = sum(1 for jid in top_k if graded_relevance.get(jid, 0) > 0)
    return relevant_in_top_k / total_relevant


def ndcg_at_k(
    ranked_ids: List[str],
    graded_relevance: Dict[str, int],
    k: int,
) -> float:
    """
    Normalized Discounted Cumulative Gain at K.

    DCG@K  = Σ(i=1..K) (2^rel_i - 1) / log2(i + 1)
    IDCG@K = DCG of the ideal (best possible) ranking
    NDCG@K = DCG@K / IDCG@K  (0.0 if IDCG == 0)
    """
    if not ranked_ids or k <= 0:
        return 0.0

    def dcg(ids: List[str], limit: int) -> float:
        gain = 0.0
        for i, jid in enumerate(ids[:limit], start=1):
            rel = graded_relevance.get(jid, 0)
            if rel > 0:
                gain += (2 ** rel - 1) / math.log2(i + 1)
        return gain

    actual_dcg = dcg(ranked_ids, k)
    ideal_ids = sorted(graded_relevance.keys(), key=lambda jid: -graded_relevance[jid])
    ideal_dcg = dcg(ideal_ids, k)

    if ideal_dcg == 0.0:
        return 0.0
    return actual_dcg / ideal_dcg


def mrr(
    ranked_ids: List[str],
    graded_relevance: Dict[str, int],
) -> float:
    """
    Mean Reciprocal Rank — reciprocal of the rank of the first relevant result.

    Returns 0.0 if no relevant item is found in the ranked list.
    """
    for i, jid in enumerate(ranked_ids, start=1):
        if graded_relevance.get(jid, 0) > 0:
            return 1.0 / i
    return 0.0


def compute_all_metrics(
    ranked_ids: List[str],
    graded_relevance: Dict[str, int],
) -> MetricsResult:
    """Compute all standard retrieval metrics from a ranked list and ground truth."""
    total_relevant = sum(1 for rel in graded_relevance.values() if rel > 0)
    return MetricsResult(
        precision_at_5=precision_at_k(ranked_ids, graded_relevance, k=5),
        precision_at_10=precision_at_k(ranked_ids, graded_relevance, k=10),
        recall_at_10=recall_at_k(ranked_ids, graded_relevance, k=10),
        ndcg_at_5=ndcg_at_k(ranked_ids, graded_relevance, k=5),
        ndcg_at_10=ndcg_at_k(ranked_ids, graded_relevance, k=10),
        mrr=mrr(ranked_ids, graded_relevance),
        total_relevant=total_relevant,
        ranked_count=len(ranked_ids),
    )
