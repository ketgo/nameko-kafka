"""
    Commits abstract base class
"""

from abc import ABC, abstractmethod


class Commits(ABC):
    """
        Abstract base class for commits
    """

    @abstractmethod
    def read(self, topic: str, partition: int) -> int:
        pass
