# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import datetime
import json
import logging
import traceback
import uuid

import redis


class TaskQueue(object):
    STATE_PENDING = 'PENDING'
    STATE_PROCESSING = 'PROCESSING'
    STATE_SUCCEEDED = 'SUCCESS'
    STATE_FAILED = 'FAILED'
    STATE_CANCEL_REQUESTED = 'CANCEL_REQUESTED'
    STATE_CANCELLED = 'CANCELLED'

    def __init__(self, queue_name, redis_host='localhost', redis_port=6379):
        self.queue_name = queue_name

        self.redis = redis.StrictRedis(host=redis_host, port=redis_port)

        self.logger = logging.getLogger(__name__)

    @property
    def _keys_prefix(self):
        return 'ltq:%s' % self.queue_name

    @property
    def _pending_jobs_key(self):
        return '%s:pending' % self._keys_prefix

    @property
    def _processing_jobs_key(self):
        return '%s:processing' % self._keys_prefix

    @property
    def _succeeded_jobs_key(self):
        return '%s:succeeded' % self._keys_prefix

    @property
    def _failed_jobs_key(self):
        return '%s:failed' % self._keys_prefix

    @property
    def _cancel_requested_jobs_key(self):
        return '%s:cancel_requested' % self._keys_prefix

    @property
    def _cancelled_jobs_key(self):
        return '%s:cancelled' % self._keys_prefix

    def _queue_from_state(self, state):
        mapping = {
            TaskQueue.STATE_PENDING: self._pending_jobs_key,
            TaskQueue.STATE_PROCESSING: self._processing_jobs_key,
            TaskQueue.STATE_FAILED: self._failed_jobs_key,
            TaskQueue.STATE_SUCCEEDED: self._succeeded_jobs_key,
            TaskQueue.STATE_CANCEL_REQUESTED: self._cancel_requested_jobs_key,
            TaskQueue.STATE_CANCELLED: self._cancelled_jobs_key,
        }
        return mapping[state]

    def enqueue_task(self, job_data):
        """Enqueue a job's data
        :param job_data: the job data
        :return: the job id
        """
        jid = '%s:job:%s' % (self._keys_prefix, str(uuid.uuid4()))

        self.redis.hmset(jid, {
            'request_time': datetime.datetime.utcnow().isoformat(),
            'job_data': json.dumps(job_data),
            'state': TaskQueue.STATE_PENDING
        })

        self.redis.lpush(self._pending_jobs_key, jid)

        return jid

    def requeue_task(self, jid):
        """Requeues a task
        :param jid: the job's id
        :return: True on success
        """
        self.set_task_state(jid, TaskQueue.STATE_PENDING)
        self.redis.hdel(jid, 'start_time', 'finish_time', 'result')
        self.redis.hincrby(jid, 'requeues')

    def work_loop(self, process_callback):
        """
        Starts a worker, and run tasks in a loop.

        The process_callback should have the signature:

        def process_callback(job_data):
            return result

        where job_data is a the task data, and result is json-serializable

        :param process_callback: the function to run on each task
        :return:
        """
        while True:
            key = self.redis.brpoplpush(
                self._pending_jobs_key,
                self._processing_jobs_key,
            )
            self.redis.hset(key, 'state', TaskQueue.STATE_PROCESSING)

            if key is None:
                continue

            self.logger.info('Job %s is starting' % key)

            try:
                h = self.redis.hgetall(key)
                job_data = json.loads(h['job_data'])

                self.register_start_time(key)

                result = process_callback(job_data)
                result_j = json.dumps(result)

            except Exception:
                self.logger.error('Job %s failed: %s' % (key, traceback.format_exc()))
                self.redis.lpush(self._failed_jobs_key, key)
                self.redis.lrem(self._processing_jobs_key, 0, key)
                self.redis.hset(key, 'state', TaskQueue.STATE_FAILED)
                self.register_finish_time(key)
            else:
                self.logger.info('Job %s succeeded' % key)
                self.redis.lpush(self._succeeded_jobs_key, key)
                self.redis.lrem(self._processing_jobs_key, 0, key)
                self.redis.hset(key, 'state', TaskQueue.STATE_SUCCEEDED)
                self.register_finish_time(key)
                self.redis.hset(key, 'result', result_j)

    def register_start_time(self, jid):
        now = datetime.datetime.utcnow().isoformat()
        self.redis.hset(jid, 'start_time', now)

    def register_finish_time(self, jid):
        now = datetime.datetime.utcnow().isoformat()
        self.redis.hset(jid, 'finish_time', now)

    def get_task_data(self, jid):
        """
        Gets the task data for a given task
        :param jid: the id of the task
        :return: the task data
        """
        jdata = self.redis.hget(jid, 'job_data')
        data = json.loads(jdata)
        return data

    def get_task_state(self, jid):
        """
        Queries the current state of a task
        :param jid: the id of the task
        :return: the state of the task
        """
        return self.redis.hget(jid, 'state')

    def get_task_result(self, jid):
        """
        Queries the result of a task
        :param jid: the id of the task
        :return: the result of the task, or None if it isn't available
        """
        if self.get_task_state(jid) == TaskQueue.STATE_SUCCEEDED:
            return json.loads(self.redis.hget(jid, 'result'))
        else:
            return None

    def cancel_task(self, jid):
        """
        Cancels a pending task. By default, this does *not* abort a running task.
        However, you are free to override it if you know how to cancel a running task.
        :param jid: the id of the task
        :return: True if the task was successfully cancelled, False if it wasn't
        """
        if self.get_task_state(jid) == TaskQueue.STATE_PENDING:
            self.set_task_state(jid, TaskQueue.STATE_CANCELLED)
            return True
        else:
            return False

    def get_pending_tasks(self):
        """
        Returns the list of ids of all pending tasks
        :return: a list of task ids
        """
        jobs = self.redis.lrange(self._pending_jobs_key, 0, -1)
        return jobs

    def _remove_old_state(self, jid):
        state = self.redis.hget(jid, 'state')
        self.redis.lrem(self._queue_from_state(state), 0, jid)

    def set_task_state(self, jid, state):
        """
        Updates the state of the given task
        :param jid: the id of the task
        :param state: the new state
        :return:
        """
        self._remove_old_state(jid)
        self.redis.hset(jid, 'state', state)
        self.redis.lpush(self._queue_from_state(state), jid)
