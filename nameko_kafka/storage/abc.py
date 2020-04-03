"""
    Abstract base class for offset storage
"""

from abc import ABC, abstractmethod


class OffsetStorage(ABC):
    """
        Abstract class exposing interface to implement different
        offset storage backends.
    """

    @abstractmethod
    def get_offset(self):
        pass

    @abstractmethod
    def store_offset(self):
        pass
