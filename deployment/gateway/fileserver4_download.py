# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import logging

import redis
from twisted.internet import defer
from twisted.internet import reactor

from renderfarm_commons.cache_client.filecache_client3 import CacheClient3

from fileserver4_path_helpers import translate_path
import settings


logger = logging.getLogger(__name__)


def sleep(seconds):
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, seconds)
    return d


class BackgroundDownloadWorker(object):
    MAX_ATTEMPTS = 3

    def __init__(self, redis_host):
        self.redis_host = redis_host

        # Setup connections to redis
        self.redis = redis.StrictRedis(host=self.redis_host, port=6379, db=0, socket_timeout=1)
        self.ssl_cert = settings.ssl_cert
        self.ssl_key = settings.ssl_key
        self.ssl_ca = settings.ssl_ca

        self.cache_client = None

    @defer.inlineCallbacks
    def pop_task(self):
        key = self.redis.rpoplpush('bkgrd_dl:pending', 'bkgrd_dl:processing')
        if key is None:
            defer.returnValue(None)

        h = self.redis.hgetall(key)
        file_path = h['path'].decode('UTF-8')
        local_path = translate_path(file_path)
        file_key = h['key']

        if self.cache_client is None:
            self.cache_client = CacheClient3(
                redis_host=self.redis_host,
                ssl_cert=self.ssl_cert,
                ssl_key=self.ssl_key,
                ssl_ca=self.ssl_ca
            )

        for _ in range(self.MAX_ATTEMPTS):
            try:
                logger.info('Starting fetch of %s into %s' % (file_key, local_path))
                yield self.cache_client.get_file(file_key, local_path, overwrite=True)
                self.redis.lpush('bkgrd_dl:succeeded', key)
                self.redis.lrem('bkgrd_dl:processing', 0, key)
                self.redis.hset(key, 'state', 'SUCCESS')
                logger.info('SUCCESS: Fetch of %s into %s' % (file_key, local_path))

                defer.returnValue(True)
            except Exception:
                logger.warning('SOFT-FAILURE: Fetch of %s into %s' % (file_key, local_path), exc_info=True)

        # If we reach here, processing has failed.
        # Put the key on the "failed" queue
        self.redis.lpush('bkgrd_dl:failed', key)
        self.redis.lrem('bkgrd_dl:processing', 0, key)
        self.redis.hset(key, 'state', 'FAILURE')
        logger.error('FAILURE: Fetch of %s into %s' % (file_key, local_path))

        defer.returnValue(False)

    @defer.inlineCallbacks
    def run_worker(self):
        while True:
            r = True
            try:
                while r is not None:
                    r = yield self.pop_task()
            except Exception:
                logger.error('Failed to get a task', exc_info=True)

            yield sleep(0.5)
