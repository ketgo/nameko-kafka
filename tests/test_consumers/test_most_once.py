"""
    Tests for at most once semantic Kafka consumer
"""

import time

import pytest
from kafka.structs import TopicPartition

from nameko_kafka.consumers.most_once import AtMostOnceConsumer


@pytest.fixture
def kafka_consumer(topic):
    _consumer = AtMostOnceConsumer(
        topic, group_id=topic, max_poll_records=2, consumer_timeout_ms=1000
    )
    yield _consumer
    _consumer.close()


def test_consumer(topic, partition, producer, kafka_consumer):
    _partition = TopicPartition(topic, partition)
    committed_offset = kafka_consumer.committed(_partition)

    producer.send(topic, b"foo-1", b"test", partition=partition)
    producer.send(topic, b"foo-2", b"test", partition=partition)
    producer.send(topic, b"foo-3", b"test", partition=partition)
    producer.send(topic, b"foo-4", b"test", partition=partition)
    time.sleep(1)

    messages = []
    committed_offsets = []

    def handler(msg):
        messages.append(msg.value)
        __partition = TopicPartition(topic, msg.partition)
        committed_offsets.append(kafka_consumer.committed(__partition))

    kafka_consumer.start(handler)

    assert messages == [b"foo-1", b"foo-2", b"foo-3", b"foo-4"]
    # Offset is committed before message is processed so the first member
    # should be one more than the very first offset when the consumer started.
    assert committed_offsets == [
        committed_offset + 2, committed_offset + 2,
        committed_offset + 4, committed_offset + 4
    ]
