# Evaluation Results

`baseline.json` — Phase 7 baseline metrics (keyword scoring + skill adjacency).

**When to regenerate:** If you change any of the following, re-run `python -m evaluation.run_eval`:
- Job descriptions in `evaluation/datasets/golden_jobs.py`
- Ground truth labels in `evaluation/datasets/ground_truth.py`
- The scoring formula in `pipeline/scorer.py` or `utils/matching_engine.py`

**Phase 8 requirement:** AI features must improve mean Precision@10 above 0.567 (current baseline).
