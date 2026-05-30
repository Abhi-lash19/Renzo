"""
Tests for pipeline/classifier.py — classify_job() function.
"""

import sys
from pathlib import Path

# Ensure project root is importable when running pytest from any directory.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import unittest

from pipeline.classifier import classify_job
from core.enums import MatchType


class TestClassifyJob(unittest.TestCase):

    # --- Score-only thresholds ---

    def test_high_score_is_strong(self):
        self.assertEqual(classify_job(8.5), "strong")

    def test_score_at_strong_threshold(self):
        """Exactly 7.0 qualifies as strong (inclusive lower bound)."""
        self.assertEqual(classify_job(7.0), "strong")

    def test_mid_score_is_stretch(self):
        self.assertEqual(classify_job(6.0), "stretch")

    def test_score_at_stretch_threshold(self):
        """Exactly 5.0 qualifies as stretch (inclusive lower bound)."""
        self.assertEqual(classify_job(5.0), "stretch")

    def test_low_score_is_learning(self):
        self.assertEqual(classify_job(3.5), "learning")

    def test_just_below_stretch_is_learning(self):
        """4.9 is below the stretch threshold and no boost — learning."""
        self.assertEqual(classify_job(4.9), "learning")

    def test_zero_score_is_learning(self):
        self.assertEqual(classify_job(0.0), "learning")

    def test_max_score_is_strong(self):
        self.assertEqual(classify_job(10.0), "strong")

    # --- Transferable-skill boost ---

    def test_transferable_boosts_learning_to_stretch(self):
        """transferable_count >= 2 promotes a learning score to stretch."""
        self.assertEqual(classify_job(3.5, transferable_count=2), "stretch")

    def test_transferable_does_not_boost_strong(self):
        """A strong score stays strong regardless of transferable_count."""
        self.assertEqual(classify_job(8.0, transferable_count=5), "strong")

    def test_transferable_does_not_affect_existing_stretch(self):
        """A score already in stretch range stays stretch with boost."""
        self.assertEqual(classify_job(6.0, transferable_count=3), "stretch")

    def test_one_transferable_does_not_boost(self):
        """transferable_count=1 is below the minimum boost threshold."""
        self.assertEqual(classify_job(4.0, transferable_count=1), "learning")

    def test_exactly_two_transferable_boosts(self):
        """TRANSFERABLE_BOOST_MIN is 2, so exactly 2 triggers the boost."""
        self.assertEqual(classify_job(2.0, transferable_count=2), "stretch")

    # --- Return type checks ---

    def test_returns_string_not_enum(self):
        """classify_job must return a plain str (MatchType.value), not a MatchType member."""
        result = classify_job(8.0)
        self.assertIsInstance(result, str)
        self.assertNotIsInstance(result, MatchType)

    def test_result_is_valid_match_type_value(self):
        """All return values must be valid MatchType values."""
        valid = {"strong", "stretch", "learning"}
        for score in [0.0, 3.5, 4.9, 5.0, 6.0, 7.0, 8.5, 10.0]:
            with self.subTest(score=score):
                self.assertIn(classify_job(score), valid)

    def test_transferable_boost_only_affects_learning_range(self):
        """Score just above stretch threshold with boost is still stretch (not double-promoted)."""
        self.assertEqual(classify_job(5.5, transferable_count=3), "stretch")


if __name__ == "__main__":
    unittest.main()
