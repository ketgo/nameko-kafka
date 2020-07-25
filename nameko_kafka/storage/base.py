"""
    Abstract base class for offset storage
"""

from abc import ABC, abstractmethod


class OffsetStorage(ABC):
    """
        Abstract class exposing interface to implement different
        offset storage backends.
    """

    def __enter__(self):
        # Buffer containing offsets to be stored. It maps topic-partition
        # tuple keys to an offset value.
        self._offsets = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def commit(self):
        offsets = getattr(self, "_offsets", {})
        self.write(offsets)

    def add(self, topic, partition, offset):
        """
            Add offset for given topic and partition to buffer for
            storage. The offset is increased by one so that the next
            un-processed message will be returned on next read by the
            consumer.

            :param topic: topic of the offset
            :param partition: topic partition of the offset
            :param offset: value of offset
        """
        self._offsets[(topic, partition)] = offset + 1

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
            Read last stored offset from storage
            for given topic and partition

            :param topic: message topic
            :param partition: partition number of the topic
            :returns: last committed offset value
        """
        raise NotImplementedError()

    @abstractmethod
    def write(self, offsets):
        """
            Write offsets to storage.

            :param offsets: mapping between topic-partition
                tuples and corresponding latest offset value
        """
        raise NotImplementedError()
