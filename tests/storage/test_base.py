"""
    Tests for offset storage base class
"""

import pytest

from nameko_kafka.storage.base import OffsetStorage


class TestStorage(OffsetStorage):

    def __init__(self):
        self.data = {}

    def setup(self):
        pass

    def stop(self):
        pass

    def read(self, topic, partition):
        offsets = self.data.get((topic, partition), [0])
        return offsets[-1]

    def write(self, offsets):
        for key, value in offsets.items():
            if key in self.data:
                self.data[key].append(value)
            else:
                self.data[key] = [value]


@pytest.fixture
def offset_storage():
    return TestStorage()


def test_add(offset_storage):
    with pytest.raises(AttributeError):
        offset_storage.add("test", 0, 0)

    with offset_storage as s:
        s.add("test", 0, 0)
        # Add increase the offset by one
        assert s._offsets == {
            ("test", 0): 1
        }


def test_commit(offset_storage):
    offset_storage.commit()
    assert offset_storage.data == {}

    with offset_storage:
        offset_storage.add("test-0", 0, 10)
        offset_storage.add("test-0", 0, 11)
        offset_storage.add("test-1", 1, 1)
        offset_storage.commit()
        assert offset_storage.data == {
            ("test-0", 0): [12],
            ("test-1", 1): [2]
        }


def test_context_manager(offset_storage):
    with offset_storage:
        offset_storage.add("test-0", 0, 10)
        offset_storage.add("test-1", 1, 1)

    with offset_storage:
        offset_storage.add("test-0", 0, 11)

    assert offset_storage.data == {
        ("test-0", 0): [11, 12],
        ("test-1", 1): [2]
    }
