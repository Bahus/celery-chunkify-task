# Efficient celery tasks chunkification

This library allows to chunkify a huge bunch of celery tasks into several numbers of chunks which will be executed periodically until the initial queue is not empty.

In other words you may split a huge amount of created tasks in the small chunks of predefined length and distribute your task creation routine among several periodic tasks.

**Real life example:** you need to send a push-notification for a zillion of your users. If you put every notification into individual task, you have to execute a zillion tasks. How to execute such amount of tasks and do not consume a lot of memory/CPU? How to avoid tasks flooding when we put all the messages in a queue at once and celery workers start producing a high load on our external/internal services?

[celery.chunks](http://docs.celeryproject.org/en/latest/userguide/canvas.html#chunks) is not an option, because it still creates all tasks in a memory and attaches a huge blob of data to a message.

Here is the example how to execute 1000 of tasks every 5 seconds:

```python
from django.db.models import Min, Max  
from chunkify import chunkify_task, Chunk  
  
users_queryset = User.objects.active()  
  
def get_initial_chunk(*args, **kwargs):
    """
    Create an chunk of integers based on max and min primary keys.
	"""
    result = users_queryset.aggregate(Min('pk'), Max('pk'))  
    chunk = Chunk(  
        start=result['pk__min'] or 0,  
        size=1000,  
        max=result['pk__max'] or 0,  
    )  
    return chunk  
  
  
@task  
@chunkify_task(  
    sleep_timeout=5,   
    initial_chunk=get_initial_chunk  
)  
def send_push_notifications(chunk: Chunk):
    """Create several tasks based on provided chunk and re-schedule their execution"""
    chunked_qs = (  
        users_queryset  
        .filter(pk__range=chunk.range)  
        .values_list('pk', flat=True)  
        .order_by('pk')   
    )     
  
    for user_id in chunked_qs:  
        send_push_notifications_for_user.delay(user_id)
```

Then the task function will be re-scheduled to run in `sleep_timeout` seconds with a next chunk.

##   chunkify_task

The decorator accepts 3 parameters:

* `sleep_timeout` – seconds between processing each chunk of tasks
* `initial_chunk` – either `Chunk`, `DateChunk` or `DateTimeChunk` instance or a callable which returns one of the specified instances.
* `chunk_class` – `Chunk`, `DateChunk` or `DateTimeChunk` type, will be used to (de)serialize chunks.

## Chunk classes

`Chunk` aka `IntChunk` – represents chunk data in list of integers, `DateChunk` and `DateTimeChunk` – represent date chunks, public API can be explorer via `BaseChunk` class.

