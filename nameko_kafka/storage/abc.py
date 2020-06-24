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
    def read(self, topic: str, partition: int) -> int:
        """
            Read offset value and return the next offset for given
            topic and partition from storage.

            :param topic: message topic
            :param partition: partition number of the topic
            :returns: last committed offset + 1
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, topic: str, partition: int, offset: int):
        """
            Write offset value for given topic and partition to storage.

            :param topic: message topic
            :param partition: partition number of the topic
            :param offset: offset value to store
        """
        raise NotImplementedError()
