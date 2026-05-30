from enum import Enum


class MatchType(str, Enum):
    """Classifies a job match as strong, stretch, or a learning opportunity."""

    STRONG = "strong"
    STRETCH = "stretch"
    LEARNING = "learning"
