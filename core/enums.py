from enum import Enum


class MatchType(str, Enum):
    STRONG = "strong"
    STRETCH = "stretch"
    LEARNING = "learning"
