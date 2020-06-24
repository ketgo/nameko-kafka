"""
    MongoDB offset storage
"""

from typing import Dict

from pymongo import MongoClient

from .abc import OffsetStorage

DEFAULT_DB_NAME = "nameko_kafka_offset"
DEFAULT_COLLECTION = "offset"


class MongoStorage(OffsetStorage):
    """
        MongoDB offset storage

        :param client: MongoDB client
        :param db_name: offset document database name
        :param collection: offset document collection name
    """

    def __init__(
            self,
            client: MongoClient,
            db_name: str,
            collection: str
    ):
        self._collection = client[db_name][collection]

    def read(self, topic: str, partition: int) -> int:
        filter_query = self._get_filter_query(topic, partition)
        document = self._collection.find_one(filter_query)
        offset = document["offset"] + 1 if document else 0

        return offset

    def write(self, topic: str, partition: int, offset: int):
        filter_query = self._get_filter_query(topic, partition)
        document = self._get_document(topic, partition, offset)
        self._collection.replace_one(filter_query, document, upsert=True)

    @staticmethod
    def _get_filter_query(topic: str, partition: int) -> Dict:
        return {
            "_id": "{}-{}".format(topic, partition)
        }

    @staticmethod
    def _get_document(topic: str, partition: int, offset: int) -> Dict:
        return {
            "_id": "{}-{}".format(topic, partition),
            "offset": offset
        }
