"""
    Tests for MongoDB storage
"""

import os

import pytest
from pymongo import MongoClient

from nameko_kafka.storage.mongo import MongoStorage, DEFAULT_DB_NAME, DEFAULT_COLLECTION_NAME

DEFAULT_MONGODB_HOST = "127.0.0.1:27017"
DB_NAME = 'nameko_kafka_test'
COLLECTION = 'offsets_test'


def create_client() -> MongoClient:
    host = os.getenv("MONGODB_HOST", DEFAULT_MONGODB_HOST)
    return MongoClient(host)


@pytest.fixture
def client():
    return create_client()


@pytest.fixture
def collection(client):
    return client[DB_NAME][COLLECTION]


@pytest.fixture
def offset_storage(client, collection):
    storage = MongoStorage(client, db_name=DB_NAME, collection=COLLECTION)
    storage.setup()
    yield storage
    storage.stop()
    collection.delete_many({})
    client.close()


def test_create(client):
    storage = MongoStorage(client)
    assert storage._db_name == DEFAULT_DB_NAME
    assert storage._collection_name == DEFAULT_COLLECTION_NAME


def test_create_error():
    with pytest.raises(TypeError):
        # Should be a MongoClient object
        MongoStorage(object())


def test_setup(client, collection):
    storage = MongoStorage(client, db_name=DB_NAME, collection=COLLECTION)
    storage.setup()
    assert storage._collection == collection


def test_stop(client):
    storage = MongoStorage(client, db_name=DB_NAME, collection=COLLECTION)
    storage.setup()
    assert storage._collection is not None
    storage.stop()
    assert storage._collection is None


def test_read(offset_storage, collection):
    doc = {"_id": "test-0", "offset": 10}
    collection.insert_one(doc)
    offset = offset_storage.read("test", 0)
    assert offset == doc["offset"]


def test_write(offset_storage, collection):
    offsets = {
        ("test", 0): 5,
        ("test", 1): 7
    }
    offset_storage.write(offsets)
    for (topic, partition), offset in offsets.items():
        _id = "{}-{}".format(topic, partition)
        doc = collection.find_one({"_id": _id})
        assert doc == {"_id": _id, "offset": offset}
