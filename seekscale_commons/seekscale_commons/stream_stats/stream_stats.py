# coding: utf-8

# Copyright Luna Technology 2015
# Matthieu Riviere <mriviere@luna-technology.com>

"""
Stream arbitrary monitoring data to a central server
"""

import json
import struct
import time

from twisted.internet import protocol
from twisted.internet import reactor


class StreamStats(protocol.Protocol):
    def connectionMade(self):
        self.factory.active_connection = self

    def connectionLost(self, reason):
        self.factory.active_connection = None

    def send_message(self, metric, timestamp, value):
        message = '%s %s %s' % (metric, str(timestamp), json.dumps(value))

        s = message
        packed_len = struct.pack('>L', len(s))
        self.transport.write(packed_len + s)


class StreamStatsFactory(protocol.ReconnectingClientFactory):
    def __init__(self):
        self.active_connection = None

    def buildProtocol(self, addr):
        self.resetDelay()
        p = StreamStats()
        p.factory = self
        return p


class StreamStatsClient(object):
    def __init__(self, remote_host, remote_port=51233):

        self.factory = StreamStatsFactory()
        reactor.connectTCP(remote_host, remote_port, self.factory)


    def send_message(self, metric, value):
        if self.factory.active_connection is not None:
            self.factory.active_connection.send_message(
                metric,
                int(time.time()),
                value)
