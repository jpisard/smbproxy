# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""This version of the file server doesn't directly serve files. Instead, it does
a multipart upload wherever the client asks.

All HTTP endpoints expect parameters (and especially file paths) to be UTF-8 encoded.
"""

import os
import sys
import json
import locale
import logging
import traceback
from functools import wraps

# Tornado-related stuff
from tornado.platform.twisted import TwistedIOLoop
from twisted.internet import reactor
TwistedIOLoop().install()

from tornado.ioloop import IOLoop
from tornado.options import parse_command_line
import tornado.web
import tornado.gen

from fileserver4_path_helpers import translate_path
import settings

VERSION = u'fileserver-4'

MAX_WORKERS = 4

logger = logging.getLogger(__name__)

from seekscale_commons.flask_utils import json_response, json_error
from seekscale_commons.cache_client.filecache_client3 import CacheClient3


def tornado_json_endpoint(func):
    @wraps(func)
    def inner(*args, **kwargs):
        s = args[0]
        try:
            ret = func(*args, **kwargs)

            if isinstance(ret, dict):
                s.write(json_response(ret))
                s.finish()
                return
            elif isinstance(ret, tuple):
                data, code = ret
                s.set_status(code)
                s.write(json_response(data))
                s.finish()
                return
            else:
                s.set_status(500)
                s.write("Configuration error!")
                s.finish()
                return
        except Exception:
            logger.exception('Exception while processing response')
            exc_string = traceback.format_exc().decode(locale.getpreferredencoding())
            s.set_status(500)
            s.write(json_error(exc_string))
            s.finish()
            return
    return inner


class StatusHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def get(self):
        ret = {
            u"version": VERSION,
        }
        return ret


class CacheFile3Json(tornado.web.RequestHandler):
    """Uploads a file into the cache"""
    @tornado.web.asynchronous
    def post(self):
        param_path = self.get_argument('path')
        path = translate_path(param_path)
        logger.info(u"cache_file3\t%s" % path)

        if self.application.cache_client is None:
            self.application.cache_client = CacheClient3(
                redis_host=self.application.redis_host,
                ssl_cert=self.application.ssl_cert,
                ssl_key=self.application.ssl_key,
                ssl_ca=self.application.ssl_ca
            )

        cache = self.application.cache_client

        d = cache.cache_file(path)

        def genResponse(ret):
            ret['status'] = 'Ok'
            self.write(json.dumps(ret, indent=4))
            self.finish()

        def handleError(err):
            ret = dict()
            ret['status'] = 'Ko'
            ret['error'] = str(err.value)
            self.set_status(500)
            self.write(json.dumps(ret, indent=4))
            self.finish()
        d.addCallbacks(genResponse, handleError)


def tornado_app(redis_host, ssl_cert, ssl_key, ssl_ca):
    twa = tornado.web.Application([
        (r'^/status.json$', StatusHandler),
        (r'^/cache_file3.json$', CacheFile3Json),
    ])

    twa.cache_client = None
    twa.redis_host = redis_host
    twa.ssl_cert = ssl_cert
    twa.ssl_key = ssl_key
    twa.ssl_ca = ssl_ca

    return twa


if __name__ == "__main__":
    parse_command_line()

    logger.info(u"Environment is %s" % repr(os.environ))
    logger.info(u"Filesystem encoding: %s" % sys.getfilesystemencoding())

    application = tornado_app(
        settings.remote_redis_host,
        settings.ssl_cert,
        settings.ssl_key,
        settings.ssl_ca
    )

    if len(sys.argv) >= 2:
        listen_port = int(sys.argv[1])
    else:
        listen_port = 15024
    application.listen(listen_port)
    reactor.run()