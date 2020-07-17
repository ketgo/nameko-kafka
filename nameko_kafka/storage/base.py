"""
    Abstract base class for offset storage
"""

from abc import ABC, abstractmethod


class OffsetRecord:
    """
        Offset record data class

        :param topic: topic to which the offset belongs
        :param partition: partition of the topic to which the offset belongs
        :param offset: value of the offset
    """

    def __init__(self, topic, partition, offset):
        self._topic = topic
        self._partition = partition
        self._offset = offset

    @property
    def topic(self):
        """
            Topic of the offset.
        """
        return self._topic

    @property
    def partition(self):
        """
            Topic partition of the offset.
        """
        return self._partition

    @property
    def offset(self):
        """
            Value of the offset.
        """
        return self._offset


class OffsetStorage(ABC):
    """
        Abstract class exposing interface to implement different
        offset storage backends.
    """

    def __init__(self):
        self._records = []

    def __enter__(self):
        self._offsets = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def commit(self):
        self.write(self._offsets)

    def add(self, topic, partition, offset):
        """
            Add offset for given topic and partition to the
            list of offsets to be stored.

            :param topic: topic of the offset
            :param partition: topic partition of the offset
            :param offset: value of offset
        """
        offset = OffsetRecord(topic, partition, offset)
        self._offsets.append(offset)

    @abstractmethod
    def setup(self):
        """
            Method for setup operations of the storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop(self):
        """
            Method to teardown operations the storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def read(self, topic, partition):
        """
            Read last stored OffsetRecord object from storage
            for given topic and partition

            :param topic: message topic
            :param partition: partition number of the topic
            :returns: last committed OffsetRecord object
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, offsets):
        """
            Write list of OffsetRecord object to storage.

            :param offsets: list of OffsetRecord objects
        """
        raise NotImplementedError()
