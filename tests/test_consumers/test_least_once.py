"""
    Tests for at least once semantic Kafka consumer
"""

from kafka.structs import TopicPartition

from nameko_kafka.consumers.least_once import AtLeastOnceConsumer


def test_consumer_without_commit_interval(topic, partition, producer):
    consumer = AtLeastOnceConsumer(topic, group_id=topic, poll_timeout_ms=100)
    _partition = TopicPartition(topic, partition)
    committed_offset = consumer.committed(_partition)
    print(committed_offset)

    # producer.send(topic, b"foo-1", b"test", partition=partition)
    # producer.send(topic, b"foo-2", b"test", partition=partition)

    messages = []
    committed_offsets = []

    def handler(msg):
        messages.append(msg)
        committed_offsets.append(consumer.committed(msg.partition))

    consumer.start(handler)
    print(messages)
    print(committed_offsets)

    # Offset is committed after message is processed so the current
    # offset should not change
    #assert consumer.committed(_partition) == offset
    #assert message.value == b"foo-1"

    # Offset should be increase by one
    #assert consumer.committed(_partition) == offset + 1
    #assert message.value == b"foo-2"


def test_consumer_with_commit_interval(topic, partition, producer):
    consumer = AtLeastOnceConsumer(topic)
