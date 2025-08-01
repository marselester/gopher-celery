"""
myproject
~~~~~~~~~

Run a Celery worker as follows:

    $ celery --app myproject worker --queues important --loglevel=debug --without-heartbeat --without-mingle

It will process tasks from "important" queue.

"""
from celery import Celery

app = Celery(broker='amqp://guest:guest@localhost:5672/')


@app.task
def mytask(a, b):
    print('received a={} b={}'.format(a, b))
