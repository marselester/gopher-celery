"""
producer
~~~~~~~~

This module sends two "myproject.mytask" tasks to "important" queue.

"""
from celery import Celery

app = Celery(
    main='myproject',
    broker='amqp://guest:guest@localhost:5672/',
)
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_TASK_PROTOCOL=2,
)


@app.task
def mytask(a, b):
    pass

mytask.apply_async(args=('fizz',), kwargs={'b': 'bazz'}, queue='important')

app.conf.update(CELERY_TASK_PROTOCOL=1)

mytask.apply_async(args=('fizz',), kwargs={'b': 'bazz'}, queue='important')
