"""
Tests for core/enums.py and core/exceptions.py — domain primitives.
"""

import sys
from pathlib import Path

# Ensure project root is importable when running pytest from any directory.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import unittest

from core.enums import MatchType
from core.exceptions import (
    RenzoError,
    ProfileLoadError,
    PipelineError,
    StorageError,
    FetchError,
    ValidationError,
)


class TestMatchType(unittest.TestCase):

    def test_strong_value(self):
        self.assertEqual(MatchType.STRONG.value, "strong")

    def test_stretch_value(self):
        self.assertEqual(MatchType.STRETCH.value, "stretch")

    def test_learning_value(self):
        self.assertEqual(MatchType.LEARNING.value, "learning")

    def test_is_string_enum(self):
        self.assertIsInstance(MatchType.STRONG, str)
        self.assertEqual(MatchType.STRONG, "strong")

    def test_all_three_members_exist(self):
        members = list(MatchType)
        self.assertEqual(len(members), 3)

    def test_match_type_in_dict_key(self):
        d = {MatchType.STRONG: 1}
        self.assertEqual(d["strong"], 1)


class TestExceptions(unittest.TestCase):

    def test_renzo_error_is_exception(self):
        self.assertTrue(issubclass(RenzoError, Exception))

    def test_profile_load_error_is_renzo_error(self):
        self.assertTrue(issubclass(ProfileLoadError, RenzoError))

    def test_pipeline_error_is_renzo_error(self):
        self.assertTrue(issubclass(PipelineError, RenzoError))

    def test_storage_error_is_renzo_error(self):
        self.assertTrue(issubclass(StorageError, RenzoError))

    def test_fetch_error_is_renzo_error(self):
        self.assertTrue(issubclass(FetchError, RenzoError))

    def test_validation_error_is_renzo_error(self):
        self.assertTrue(issubclass(ValidationError, RenzoError))

    def test_can_raise_and_catch_as_renzo_error(self):
        with self.assertRaises(RenzoError):
            raise ProfileLoadError("test")

    def test_exception_message_preserved(self):
        self.assertEqual(str(ProfileLoadError("msg")), "msg")


if __name__ == "__main__":
    unittest.main()
