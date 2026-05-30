from core.enums import MatchType

STRONG_THRESHOLD = 7.0
STRETCH_THRESHOLD = 5.0
TRANSFERABLE_BOOST_MIN = 2


def classify_job(score: float, transferable_count: int = 0) -> str:
    """
    Classify a job as strong, stretch, or learning based on score and transferable skill count.

    Score thresholds are the primary gate. transferable_count >= TRANSFERABLE_BOOST_MIN
    promotes a learning job (score < STRETCH_THRESHOLD) to stretch.
    This boost does not apply to jobs that already qualify as strong.
    """
    if score >= STRONG_THRESHOLD:
        return MatchType.STRONG.value
    if score >= STRETCH_THRESHOLD or transferable_count >= TRANSFERABLE_BOOST_MIN:
        return MatchType.STRETCH.value
    return MatchType.LEARNING.value
