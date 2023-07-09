"""
    Tests for kafka entrypoint
"""

import json
import time

import pytest
from nameko.testing.services import entrypoint_waiter

from nameko_kafka import consume, KafkaConsumer, Semantic
from nameko_kafka.constants import KAFKA_CONSUMER_CONFIG_KEY
from nameko_kafka.storage import MongoStorage
from .test_storage.test_mongo import create_client, DB_NAME, COLLECTION


@pytest.fixture
def service_cls(topic):
    class Service:
        name = "kafka-test"

        @consume(topic, group_id="test")
        def check_message(self, message):
            return message.value

        @consume(topic + "_least_once", semantic=Semantic.AT_LEAST_ONCE, group_id="test_least_once")
        def check_message_least_once(self, message):
            return message.value + "_least_once".encode()

        @consume(topic + "_most_once", semantic=Semantic.AT_MOST_ONCE, group_id="test_most_once")
        def check_message_most_once(self, message):
            return message.value + "_most_once".encode()

        @consume(
            topic + "_exactly_once_mongodb",
            semantic=Semantic.EXACTLY_ONCE,
            group_id="test_exactly_once_mongodb",
            storage=MongoStorage(create_client(), db_name=DB_NAME, collection=COLLECTION)
        )
        def check_message_exactly_once_mongodb(self, message):
            return message.value + "_exactly_once_mongodb".encode()

    return Service


@pytest.fixture
def container(container_factory, service_cls):
    container = container_factory(service_cls, {})
    container.start()
    yield container
    container.stop()


@pytest.mark.parametrize("suffix", [
    "", "_least_once", "_most_once", "_exactly_once_mongodb"
])
def test_entrypoint(container, producer, topic, partition, wait_for_result, entrypoint_tracker, suffix):
    with entrypoint_waiter(container, "check_message{}".format(suffix), callback=wait_for_result, timeout=30):
        time.sleep(1)
        producer.send(topic + suffix, b"foo-1", b"test", partition=partition)
        producer.send(topic + suffix, b"foo-2", b"test", partition=partition)

    assert entrypoint_tracker.get_results() == [
        b"foo-1" + suffix.encode(), b"foo-2" + suffix.encode()
    ]


@pytest.mark.parametrize("file_config,env_config,kwargs,kafka_config", [
    (
            {},
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_args",
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_args",
                "retry_backoff_ms": 3
            }
    ),
    (
            {
                KAFKA_CONSUMER_CONFIG_KEY: {
                    "bootstrap_servers": "mongodb://localhost/default_file",
                    "retry_backoff_ms": 5
                }
            },
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {},
            {
                "bootstrap_servers": "mongodb://localhost/default_file",
                "retry_backoff_ms": 5
            }
    ),
    (
            {
                KAFKA_CONSUMER_CONFIG_KEY: {
                    "bootstrap_servers": "mongodb://localhost/default_file",
                    "retry_backoff_ms": 5
                }
            },
            {
                KAFKA_CONSUMER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "mongodb://localhost/default_env",
                    "retry_backoff_ms": 3
                })
            },
            {
                "retry_backoff_ms": 4
            },
            {
                "bootstrap_servers": "mongodb://localhost/default_file",
                "retry_backoff_ms": 4
            }
    ),

])
def test_config(topic, mocker, mock_container, file_config, env_config, kwargs, kafka_config):
    mocker.patch('os.environ', env_config)
    dep = KafkaConsumer(topic, **kwargs).bind(mock_container, 'engine')
    dep.container.config = file_config
    assert dep._parse_config() == kafka_config


def test_entrypoint_config_error(topic):
    with pytest.raises(TypeError):
        KafkaConsumer(topic, **{"test": 0})
