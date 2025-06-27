"""
producer
~~~~~~~~

This module sends a "myproject.mytask" task to "important" queue.

"""
import argparse

from celery import Celery

parser = argparse.ArgumentParser()
parser.add_argument('--protocol', type=int, default=2, help='Celery protocol version')
args = parser.parse_args()

app = Celery(
    main='myproject',
    broker='redis://localhost:6379',
)
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_TASK_PROTOCOL=args.protocol,
)


@app.task
def mytask(a, b):
    pass

mytask.apply_async(args=('fizz',), kwargs={'b': 'bazz'}, queue='important')
