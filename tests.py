import pytest

import celery
from chunkify import Chunk, ChunkMode, chunkify_task

import logging

logging.basicConfig(level=logging.DEBUG)

celery_app = celery.Celery(broker='memory://', backend='cache+memory://')


class Config:
    task_always_eager = True


celery_app.config_from_object(Config)


class TestChunkObject:

    @pytest.mark.parametrize(
        ['chunk', 'expected_end'], [
            (Chunk(0, 10, 100), 10),
            (Chunk(1, 11, None), 12),
            (Chunk(1, 10, 5), 5),
        ]
    )
    def test_end(self, chunk, expected_end):
        assert chunk.end == expected_end

    @pytest.mark.parametrize(
        ['chunk', 'expected_slice'], [
            (Chunk(0, 10, 100), slice(0, 10)),
            (Chunk(1, 11, None), slice(1, 12)),
            (Chunk(1, 10, 5), slice(1, 5)),
        ]
    )
    def test_slicing(self, chunk, expected_slice):
        assert chunk.slice == expected_slice

    @pytest.mark.parametrize(
        ['chunk', 'is_exhausted'], [
            (Chunk(0, 10, 100), False),
            (Chunk(1, 11, None), False),
            (Chunk(100, 11, None), False),
            (Chunk(100, 10, 100), False),
            (Chunk(100, 10, 100, ChunkMode.SLICE), True),
            (Chunk(100, 10, 91), True),
            (Chunk(100, 10, 91, ChunkMode.SLICE), True),
        ]
    )
    def test_exhausted(self, chunk, is_exhausted):
        assert chunk.is_exhausted == is_exhausted

    def test_full_iteration(self):
        values = list(range(1, 100, 2))

        chunk = Chunk(0, 10, len(values), ChunkMode.SLICE)

        count = 0
        iterated_values = []

        while not chunk.is_exhausted:
            iterated_values.extend(values[chunk.slice])
            chunk = chunk.next()
            count += 1

        assert iterated_values == values
        assert count == len(values) / chunk.size


items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
items_len = len(items)


@celery_app.task
def remove_item(index):
    items[index] = None


@celery_app.task
@chunkify_task(
    sleep_timeout=10,
    initial_chunk=Chunk(0, 3, 10, ChunkMode.SLICE),
)
def runner_list(chunk: Chunk):
    for i in list(range(items_len))[chunk.slice]:
        remove_item.delay(i)


class TestChunkificator:

    def test_list_chunkificator(self):
        runner_list.delay()
        assert [i for i in items if i is not None] == [11, 12]
