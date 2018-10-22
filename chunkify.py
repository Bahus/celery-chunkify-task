import datetime
from functools import wraps
from typing import Any, NamedTuple, Optional, Callable, Union, Type

import structlog
from celery import current_task


logger = structlog.get_logger(__name__)
seconds = int


class ChunkMode:
    RANGE = 'range'
    SLICE = 'slice'


class BaseChunk:
    """Abstraction above chunk objects.

    :param start: start point of a chunk
    :param size: size of a chunk
    :param max: maximum value `start` parameter can get for a chunk
    :param ChunkMode mode: declares how this chunk will be used
    """
    start: Any
    size: Any
    max: Optional[Any]
    mode: str = ChunkMode.RANGE

    @property
    def end(self):
        end = self.start + self.size

        if self.max and end > self.max:
            return self.max

        return self.start + self.size

    @property
    def range(self):
        return self.start, self.end

    @property
    def slice(self):
        return slice(self.start, self.end)

    @property
    def is_exhausted(self):
        if self.mode == ChunkMode.SLICE:
            return bool(self.max is not None and self.start >= self.max)

        return bool(self.max is not None and self.start > self.max)

    @property
    def shift_getter(self) -> Callable:
        raise NotImplementedError()

    def next(self, shift: int=None):
        if shift is None:
            if self.mode == ChunkMode.SLICE:
                shift = self.shift_getter(0)
            else:
                shift = self.shift_getter(1)

        return type(self)(
            start=self.start + self.size + shift,
            size=self.size,
            max=self.max,
            mode=self.mode,
        )


class IntChunkMixin(NamedTuple):
    start: int
    size: int
    max: Optional[int]
    mode: str = ChunkMode.RANGE


class IntChunk(IntChunkMixin, BaseChunk):

    @property
    def shift_getter(self):
        return int


# backward compatibility
Chunk = IntChunk


class DateChunkMixin(NamedTuple):
    start: datetime.date
    size: datetime.timedelta
    max: Optional[datetime.date]
    mode: str = ChunkMode.RANGE


class DateChunk(DateChunkMixin, BaseChunk):

    @property
    def shift_getter(self):
        return lambda shift: datetime.timedelta(days=shift)


class DatetimeChunkMixin(NamedTuple):
    start: datetime.datetime
    size: datetime.timedelta
    max: Optional[datetime.datetime]
    mode: str = ChunkMode.RANGE


class DateTimeChunk(DatetimeChunkMixin, BaseChunk):

    @property
    def shift_getter(self):
        return lambda shift: datetime.timedelta(seconds=shift)


class chunkify_task:
    """
    This decorator allows to chunkify a huge bunch of celery tasks into several
    number of chunks which will be executed periodically until the initial queue
    is not empty.

    In other words you may split a huge amount of created tasks in the small
    chunks of predefined length and distribute your task creation routine
    among several periodic tasks.

    How to use:

    @celery_app.task(...params...)
    @chunkify_task(sleep_timeout=10, initial_chunk=Chunk(0, 100, 1000))
    def sync_provider_accounts(chunk: Chunk=None):
        # create several tasks based on provided chunk
        # schedule their execution
        return ...

    Then the task function will be re-scheduled to run in `sleep_timeout`
    seconds with a next chunk.

    Where:

        * sleep_timeout – seconds between processing each chunk of tasks.
        * initial_chunk – either `Chunk` object or callable which should return one.
            Allows to specify first value in a chunk, chunk size and maximum
            number of tasks.
    """
    def __init__(self, *,
                 sleep_timeout: seconds,
                 initial_chunk: Union[BaseChunk, Callable],
                 chunk_class: Type[BaseChunk]=Chunk):
        self.sleep_timeout = sleep_timeout
        self.initial_chunk = initial_chunk
        self.logger = logger
        self.task_function = None
        self.chunk_class = chunk_class

    def __call__(self, task_function, *args, **kwargs):
        self.task_function = task_function

        @wraps(task_function)
        def _wrapper(*args, **kwargs):
            return self._task_function_wrapper(*args, **kwargs)
        return _wrapper

    def _task_function_wrapper(self, *args, **kwargs):
        chunk = self._get_or_create_chunk(args, kwargs)

        log = self.logger.bind(
            chunk=chunk,
            name=current_task.name,
        )

        log.info('Executing chunked task')

        kwargs['chunk'] = chunk

        result = self.task_function(*args, **kwargs)

        next_chunk = chunk.next()
        if next_chunk.is_exhausted:
            log.info('Chunk is exhausted, iteration stopped',
                     next_chunk=next_chunk)

            return result

        kwargs['chunk'] = next_chunk
        current_task.apply_async(
            kwargs=kwargs, countdown=self.sleep_timeout
        )

        log.info('Next chunk is scheduled',
                 next_chunk=next_chunk,
                 sleep_timeout=self.sleep_timeout)

        return result

    def _get_or_create_chunk(self, args, kwargs):
        chunk = kwargs.get('chunk')

        # first task execution – we should create an initial chunk
        # and pass it to a task function
        if not chunk:
            return self._get_initial_chunk(args, kwargs)

        if isinstance(chunk, BaseChunk):
            return chunk
        elif isinstance(chunk, dict):
            return self.chunk_class(**chunk)
        elif isinstance(chunk, (list, tuple)):
            return self.chunk_class(*chunk)

        raise TypeError(f'Unexpected chunk type "{type(chunk)}"')

    def _get_initial_chunk(self, args, kwargs) -> Chunk:
        if callable(self.initial_chunk):
            return self.initial_chunk(*args, **kwargs)
        return self.initial_chunk

