"""
    Tests for at exactly once semantic Kafka consumer
"""

import time

import pytest

from nameko_kafka.consumers.exactly_once import ExactlyOnceConsumer
from nameko_kafka.storage.base import OffsetStorage


class MockStorage(OffsetStorage):

    def __init__(self):
        self.data = {}

    def setup(self):
        pass

    def stop(self):
        pass

    def read(self, topic, partition):
        offsets = self.data.get((topic, partition), [0])
        return offsets[-1]

    def write(self, offsets):
        for key, value in offsets.items():
            if key in self.data:
                self.data[key].append(value)
            else:
                self.data[key] = [value]


@pytest.fixture
def offset_storage():
    return MockStorage()


@pytest.fixture
def kafka_consumer(random_topic, offset_storage, kafka_admin):
    _consumer = ExactlyOnceConsumer(
        random_topic,
        storage=offset_storage,
        group_id=random_topic,
        max_poll_records=2,
        consumer_timeout_ms=1000
    )
    yield _consumer
    kafka_admin.delete_topics([random_topic])
    _consumer.close()


def test_consumer(random_topic, partition, producer, kafka_consumer, offset_storage):
    committed_offset = offset_storage.read(random_topic, partition)

    producer.send(random_topic, b"foo-1", b"test", partition=partition)
    producer.send(random_topic, b"foo-2", b"test", partition=partition)
    producer.send(random_topic, b"foo-3", b"test", partition=partition)
    producer.send(random_topic, b"foo-4", b"test", partition=partition)
    time.sleep(1)

    messages = []
    committed_offsets = []

    def handler(msg):
        messages.append(msg.value)
        committed_offsets.append(offset_storage.read(random_topic, partition))

    kafka_consumer.start(handler)

    assert messages == [b"foo-1", b"foo-2", b"foo-3", b"foo-4"]
    # Offset is committed after message is processed so the first member
    # should be the very first offset when the consumer started.
    assert committed_offsets == [
        committed_offset, committed_offset,
        committed_offset + 2, committed_offset + 2
    ]
