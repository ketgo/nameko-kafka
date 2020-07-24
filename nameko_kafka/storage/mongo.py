"""
    MongoDB offset storage
"""

from pymongo import MongoClient

from .base import OffsetStorage

DEFAULT_DB_NAME = "nameko_kafka_offsets"
DEFAULT_COLLECTION_NAME = "offsets"


class MongoStorage(OffsetStorage):
    """
        MongoDB offset storage

        :param client: MongoDB client
        :param db_name: offset document database name
        :param collection: offset document collection name
    """

    def __init__(self, client, db_name=None, collection=None):
        if not isinstance(client, MongoClient):
            raise TypeError(
                "'MongoClient' object required by the storage. "
                "Invalid object of type '{}' given.".format(type(client))
            )
        self._client = client
        self._db_name = db_name or DEFAULT_DB_NAME
        self._collection_name = collection or DEFAULT_COLLECTION_NAME
        self._collection = None

    def setup(self):
        self._collection = self._client[self._db_name][self._collection_name]

    def stop(self):
        self._collection = None
        self._client.close()

    def read(self, topic, partition):
        filter_query = self._get_filter_query(topic, partition)
        document = self._collection.find_one(filter_query)
        offset = document["offset"] if document else 0

        return offset

    def write(self, offsets):
        for (topic, partition), offset in offsets.items():
            self._write(topic, partition, offset)

    def _write(self, topic, partition, offset):
        filter_query = self._get_filter_query(topic, partition)
        document = self._get_document(topic, partition, offset)
        self._collection.replace_one(filter_query, document, upsert=True)

    @staticmethod
    def _get_filter_query(topic, partition):
        return {
            "_id": "{}-{}".format(topic, partition)
        }

    @staticmethod
    def _get_document(topic, partition, offset):
        return {
            "_id": "{}-{}".format(topic, partition),
            "offset": offset
        }
