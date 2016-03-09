# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""This version of the file server doesn't directly serve files. Instead, it does
a multipart upload wherever the client asks.

All HTTP endpoints expect parameters (and especially file paths) to be UTF-8 encoded.
"""

import os
import sys
import logging

from twisted.internet import reactor

from fileserver4_download import BackgroundDownloadWorker
import settings

VERSION = u'fileserver-4'

logger = logging.getLogger(__name__)


def setup_background_workers():
    redis_host = settings.remote_host
    bg_dl_worker = BackgroundDownloadWorker(redis_host)
    bg_dl_worker.run_worker()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logger.info(u"Environment is %s" % repr(os.environ))
    logger.info(u"Filesystem encoding: %s" % sys.getfilesystemencoding())

    setup_background_workers()
    reactor.run()
