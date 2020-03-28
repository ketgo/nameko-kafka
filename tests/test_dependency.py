"""
    Tests for kafka dependency
"""

import json

import pytest
from nameko.testing.services import dummy, entrypoint_hook

from nameko_kafka import KafkaProducer
from nameko_kafka.constants import KAFKA_PRODUCER_CONFIG_KEY


@pytest.fixture
def service_cls(topic, partition):
    class Service:
        name = "kafka-test"
        producer = KafkaProducer()

        @dummy
        def dispatch(self, message, key):
            self.producer.send(topic, value=message, key=key, partition=partition)

    return Service


@pytest.fixture
def container(container_factory, service_cls, topic):
    container = container_factory(service_cls, {})
    container.start()
    yield container
    container.stop()


def test_dependency(container, consumer):
    with entrypoint_hook(container, "dispatch") as hook:
        hook(b"foo", b"test")
        fetch = consumer.poll(timeout_ms=1000, max_records=1)
        for topic in fetch:
            for msg in fetch[topic]:
                assert msg.value == b"foo"
                assert msg.key == b"test"


@pytest.mark.parametrize("file_config,env_config,kwargs,kafka_config", [
    (
            {},
            {
                KAFKA_PRODUCER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "kafka_env",
                    "retries": 3
                })
            },
            {
                "bootstrap_servers": "kafka_args",
            },
            {
                "bootstrap_servers": "kafka_args",
                "retries": 3
            }
    ),
    (
            {
                KAFKA_PRODUCER_CONFIG_KEY: {
                    "bootstrap_servers": "kafka_file",
                    "retries": 5
                }
            },
            {
                KAFKA_PRODUCER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "kafka_env",
                    "retries": 3
                })
            },
            {},
            {
                "bootstrap_servers": "kafka_file",
                "retries": 5
            }
    ),
    (
            {
                KAFKA_PRODUCER_CONFIG_KEY: {
                    "bootstrap_servers": "kafka_file",
                    "retries": 5
                }
            },
            {
                KAFKA_PRODUCER_CONFIG_KEY: json.dumps({
                    "bootstrap_servers": "kafka_env",
                    "retries": 3
                })
            },
            {
                "retries": 4
            },
            {
                "bootstrap_servers": "kafka_file",
                "retries": 4
            }
    ),

])
def test_config(mocker, mock_container, file_config, env_config, kwargs, kafka_config):
    mocker.patch('os.environ', env_config)
    dep = KafkaProducer(**kwargs).bind(mock_container, 'engine')
    dep.container.config = file_config
    assert dep._parse_config() == kafka_config


def test_dependency_config_error():
    with pytest.raises(TypeError):
        KafkaProducer(**{"test": 0})
