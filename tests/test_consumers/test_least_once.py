"""
    Tests for at least once semantic Kafka consumer
"""

import pytest
from kafka.structs import TopicPartition

from nameko_kafka.consumers.least_once import AtLeastOnceConsumer


@pytest.fixture
def consumer(topic):
    _consumer = AtLeastOnceConsumer(
        topic, group_id=topic, poll_timeout_ms=1000, max_poll_records=1
    )
    yield _consumer
    _consumer.close()


def test_consumer(topic, partition, producer, consumer):
    _partition = TopicPartition(topic, partition)
    committed_offset = consumer.committed(_partition)

    producer.send(topic, b"foo-1", b"test", partition=partition)
    producer.send(topic, b"foo-2", b"test", partition=partition)
    producer.flush()

    messages = []
    committed_offsets = []

    def handler(msg):
        messages.append(msg.value)
        __partition = TopicPartition(topic, msg.partition)
        committed_offsets.append(consumer.committed(__partition))

    consumer.start(handler)

    assert messages == [b"foo-1", b"foo-2"]
    # Offset is committed after message is processed so the first member
    # should be the very first offset when the consumer started.
    assert committed_offsets == [committed_offset, committed_offset + 1]
