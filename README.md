# Nameko-Kafka

[![Build Status](https://travis-ci.com/ketgo/nameko-kafka.svg?branch=master)](https://travis-ci.com/ketgo/nameko-kafka)
[![codecov.io](https://codecov.io/gh/ketgo/nameko-kafka/coverage.svg?branch=master)](https://codecov.io/gh/ketgo/nameko-kafka/coverage.svg?branch=master)
[![MIT licensed](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
---

Kafka extension for [Nameko](https://www.nameko.io/) microservice framework. 

## Introduction

This is a Nameko microservice framework [extension](https://nameko.readthedocs.io/en/stable/key_concepts.html) to support 
Kafka entrypoint and dependency. The motivation behind the project is issue [569](https://github.com/nameko/nameko/issues/569). 
_Nameko-kafka_ provide a simple implementation of the entrypoint based on the approach by [calumpeterwebb](https://medium.com/@calumpeterwebb/nameko-tutorial-creating-a-kafka-consuming-microservice-c4a7adb804d0).
It also includes a dependency provider for publishing Kafka messages from within a Nameko service.

## Installation

The package is supports Python >= 3.5
```bash
$ pip install nameko-kafka
```

## Usage

The extension can be used for both, a service dependency and entrypoint. Example usage for both cases are shown in the
following sections.

## Dependency

This is basically a [python-kafka](https://github.com/dpkp/kafka-python) producer in the form of Nameko dependency. 
Nameko uses dependency injection to instantiate the producer. You just need to declare it in your service class as shown:

```python
from nameko.rpc import rpc
from nameko_kafka import KafkaProducer


class MyService:
    """
        My microservice
    """
    name = "my-service"
    # Kafak dependency
    producer = KafkaProducer(bootstrap_servers='localhost:1234')
    
    @rpc
    def method(self):
        # Publish message using dependency
        self.producer.send("kafka-topic", value=b"my-message", key=b"my-key")
```

Here `KafkaProducer` accepts all options valid for `python-kafka`'s [KafkaProducer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html).

### Entrypoint

You can use the `nameko_kafka.consume` decorator in your services to process Kafka messages:

```python
from nameko_kafka import consume


class MyService:
    """
        My microservice 
    """
    name = "my-service"

    @consume("kafka-topic", group_id="my-group", bootstrap_servers='localhost:1234')
    def method(self, message):
        # Your message handler
        handle_message(message) 
```

The `consume` decorator accepts all the options valid for `python-kafka`'s [KafkaConsumer](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html). 

On top of the default `python-kafka`'s autocommit feature, the entrypoint also comes with support for three different 
types of offset commit strategies: _at least once_, _at most once_ and _exactly once_. The three strategies correspond 
to the different message delivery semantics achievable in Kafka. Examples for each are shown in the following subsections.

#### At Least Once

```python
from nameko_kafka import consume, Semantic


class MyService:
    """
        My microservice 
    """
    name = "my-service"
    
    # At least once semantic consumer
    @consume("kafka-topic", group_id="my-group", bootstrap_servers='localhost:1234', semantic=Semantic.AT_LEAST_ONCE)
    def method(self, message):
        # Your message handler
        handle_message(message) 
```

#### At Most Once

```python
from nameko_kafka import consume, Semantic


class MyService:
    """
        My microservice 
    """
    name = "my-service"
    
    # At most once semantic consumer
    @consume("kafka-topic", group_id="my-group", bootstrap_servers='localhost:1234', semantic=Semantic.AT_MOST_ONCE)
    def method(self, message):
        # Your message handler
        handle_message(message) 
```

#### Exactly Once

The exactly once semantic requires a persistent storage to save message offsets. Such a persistent store can be 
implemented using the `OffsetStorage` interface provided by Nameko-kafka. There can be various backend implementations 
like RDBMS, NoSQL databases, etc. Support for some comes out of the box:

##### MongoDB Storage 

```python
from nameko_kafka import consume, Semantic
from nameko_kafka.storage import MongoStorage

from pymongo import MongoClient


class MyService:
    """
        My microservice 
    """
    name = "my-service"
    
    # At most once semantic consumer
    @consume(
        "kafka-topic", 
        group_id="my-group", 
        bootstrap_servers='localhost:1234', 
        semantic=Semantic.EXACTLY_ONCE,
        storage=MongoStorage(
            # MongoDB backend client
            client=MongoClient('localhost', 27017),
            # Database to use for storage
            db_name="database-name",
            # Collection to use for storage
            collection="collection-name"
        )       
    )
    def method(self, message):
        # Your message handler
        handle_message(message) 
```

Note: If the `db_name` and `collection` arguments are not specified, the default value of `"nameko_kafka_offsets"` and 
`"offsets"` will be used by the storage respectively.

##### SQL Storage

Part of v0.2.1

##### S3 Storage

Part of v0.2.2

##### Azure Block Storage

Part of v0.2.3

##### Create Custom Storage

You can create your own offset storage by implementing the `OffsetStorage` interface. It exposes the following methods:

```python
from nameko_kafka.storage.base import OffsetStorage

class MyStorage(OffsetStorage):
    """
        My custom offset storage.
    """

    def setup(self):
        """
            Method for setup of the storage.
        """

    def stop(self):
        """
            Method to teardown the storage.
        """

    def read(self, topic, partition):
        """
            Read last stored offset from storage for 
            given topic and partition.

            :param topic: message topic
            :param partition: partition number of the topic
            :returns: last committed offset value
        """

    def write(self, offsets):
        """
            Write offsets to storage.

            :param offsets: mapping between topic-partition
                tuples and corresponding latest offset value, 
                e.g.
                {
                    ("topic-1", 0): 1,
                    ("topic-1", 1): 3,
                    ("topic-2", 1): 10,
                    ...
                }
        """
```


## Configurations

The extension configurations can be set in a nameko [config.yaml]((https://docs.nameko.io/en/stable/cli.html)) file, or 
by environment variables.

### Config File

```yaml
# Config for entrypoint
KAFKA_CONSUMER:
  bootstrap_servers: 'localhost:1234'
  retry_backoff_ms: 100
  ...

# Config for dependency
KAFKA_PRODUCER:
  bootstrap_servers: 'localhost:1234'
  retries: 3
  ...
```

### Environment Variables

```.env
# Config for entrypoint
KAFKA_CONSUMER='{"bootstrap_servers": "localhost:1234", "retry_backoff_ms": 100}'

# Config for dependency
KAFKA_PRODUCER='{"bootstrap_servers": "localhost:1234", "retries": 3}'
```

## Milestones

- [x] Kafka Entrypoint
- [x] Kafka Dependency
- [x] Commit strategies: 
    - _ALMOST_ONCE_DELIVERY_
    - _AT_LEAST_ONCE_DELIVERY_ 
    - _EXACTLY_ONCE_DELIVERY_
- [x] Commit storage for _EXACT_ONCE_DELIVERY_ strategy

## Developers

For development a kafka broker is required. You can spawn one using the [docker-compose.yml](https://github.com/ketgo/nameko-kafka/blob/master/tests/conftest.py) 
file in the `tests` folder:
```bash
$ cd tests
$ docker-compose up -d 
```

To install all package dependencies:
```bash
$ pip install -r .[dev]
or
$ make deps
```

Other useful commands:
```bash
$ pytest --cov=nameko_kafka tests/			# to get coverage report
or
$ make coverage

$ pylint nameko_kafka       # to check code quality with PyLint
or
$ make lint
```

## Contributions

Issue reports and Pull requests are always welcomed. Thanks!
