"""
producer
~~~~~~~~

This module sends a "myproject.mytask" task to the "important" queue, with Celery protocol version 2.

"""
from celery import Celery

app = Celery(
    main='myproject',
    broker='redis://localhost:6379',
)
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_TASK_PROTOCOL=2,
)


@app.task
def mytask(a, b):
    pass

mytask.apply_async(args=('fizz',), kwargs={'b': 'bazz'}, queue='important')
