# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import time
from unittest import TestCase

import redis

from . import task_queue


class RedisMock(object):
    data = dict()

    def __init__(self, *args, **kwargs):
        pass

    def hmset(self, key, hm):
        self.data[key] = hm

    def lpush(self, key, value):
        if key not in self.data:
            self.data[key] = []

        self.data[key].insert(0, value)

    def brpoplpush(self, in_list, out_list, timeout=0):
        i = 0
        while i < timeout:
            if in_list in self.data and len(self.data[in_list]) > 0:
                v = self.data[in_list].pop()
                self.lpush(out_list, v)
                break
            else:
                time.sleep(1)
            i += 1

    def hget(self, key, subkey):
        if key not in self.data:
            return None
        else:
            return self.data[key].get(subkey, None)

    def hset(self, key, subkey, value):
        if key not in self.data:
            self.data[key] = dict()

        self.data[key][subkey] = value

    def lrem(self, key, count, value):
        if not key in self.data or not isinstance(self.data[key], list):
            return 0
        else:
            new_data = []
            ret = 0
            for v in self.data[key]:
                if v != value:
                    new_data.append(v)
                else:
                    ret += 1
            self.data[key] = new_data
            return ret

    def lrange(self, key, start, end):
        if not key in self.data:
            return []
        else:
            if end < 0:
                end = len(self.data[key]) + 1 + end
            return self.data[key][start:end]

    @classmethod
    def flushall(cls):
        cls.data = dict()


redis.StrictRedis = RedisMock


class TaskQueueTest(TestCase):
    def setUp(self):
        RedisMock.flushall()

    def test_task_queue(self):
        t = task_queue.TaskQueue('test_task_queue')

        job_data = {
            'value': 667
        }

        jid = t.enqueue_task(job_data)

        self.assertEquals(t.get_task_state(jid), t.STATE_PENDING)
        self.assertListEqual(t.get_pending_tasks(), [jid])

        t.set_task_state(jid, t.STATE_FAILED)
        self.assertEquals(t.get_task_state(jid), t.STATE_FAILED)
        self.assertListEqual(t.get_pending_tasks(), [])
        self.assertListEqual(t.redis.lrange(t._failed_jobs_key, 0, -1), [jid])

        self.assertDictEqual(t.get_task_data(jid), job_data)

    def test_task_cancellation(self):
        t = task_queue.TaskQueue('test_task_queue')

        job_data = {
            'value': 9292,
        }

        jid = t.enqueue_task(job_data)
        v = t.cancel_task(jid)

        self.assertTrue(v)
        self.assertListEqual(t.redis.lrange(t._pending_jobs_key, 0, 1), [])
        self.assertListEqual(t.redis.lrange(t._cancelled_jobs_key, 0, -1), [jid])

    def test_task_cancellation_nonexisting_job(self):
        t = task_queue.TaskQueue('test_task_queue')

        v = t.cancel_task('WootWootWootPumpItUp!!!')

        self.assertFalse(v)
        self.assertListEqual(t.redis.lrange(t._cancelled_jobs_key, 0, -1), [])