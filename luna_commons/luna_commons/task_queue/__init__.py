# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from .task_queue import TaskQueue

STATE_PENDING = TaskQueue.STATE_PENDING
STATE_PROCESSING = TaskQueue.STATE_PROCESSING
STATE_SUCCEEDED = TaskQueue.STATE_SUCCEEDED
STATE_FAILED = TaskQueue.STATE_FAILED