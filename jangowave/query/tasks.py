# Create your tasks here
from __future__ import absolute_import, unicode_literals

from celery import shared_task
import celery

@shared_task
def add(x, y):
    return x + y


@shared_task
def mul(x, y):
    return x * y

@celery.task()
def add_together(a, b):
    return a + b
