# coding: utf-8

# Copyright Luna Technology 2015
# Matthieu Riviere <mriviere@luna-technology.com>

import os
import platform

from statsd import StatsClient

import settings


class StatsdClient(object):
    stats_client = None

    @classmethod
    def get(cls):
        if cls.stats_client is None:
            cls.stats_client = StatsClient(
                host=settings.STATSD_HOST,
                port=settings.STATSD_PORT,
                prefix='%s.smbproxy.%d' % (platform.node(), os.getpid()),
                maxudpsize=512
            )
        return cls.stats_client
