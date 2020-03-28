"""
    Testing utility fixtures
"""

import pytest
from kafka import KafkaConsumer as Consumer
from kafka import KafkaProducer as Producer


@pytest.fixture
def topic():
    return "test"


@pytest.fixture
def partition():
    return 0


@pytest.fixture
def producer():
    producer = Producer()
    yield producer
    producer.close()


@pytest.fixture
def consumer(topic):
    consumer = Consumer(topic, group_id=topic)
    yield consumer
    consumer.close()


@pytest.fixture
def entrypoint_tracker():
    class CallTracker(object):

        def __init__(self):
            self.calls = []

        def __len__(self):
            return len(self.calls)

        def track(self, **call):
            self.calls.append(call)

        def get_results(self):
            return [call['result'] for call in self.calls]

        def get_exceptions(self):
            return [call['exc_info'] for call in self.calls]

    return CallTracker()


@pytest.fixture
def wait_for_result(entrypoint_tracker):
    def cb(worker_ctx, res, exc_info):
        entrypoint_tracker.track(result=res, exc_info=exc_info)
        return True

    return cb
