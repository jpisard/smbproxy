#!/usr/bin/env python
# coding: utf-8

# Copyright Luna Technology 2015
# Matthieu Riviere <mriviere@luna-technology.com>

from functools import wraps
import json
import locale
import logging
import traceback
import urllib

from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line
import tornado.web


from metadata_loader import (
    get_cached_list_dir,
    set_cached_list_dir,
    get_cached_file_metadata,
    set_cached_file_metadata,
)
from smbproxy4 import settings


LISTEN_PORT = 25200
FILESERVER_HOST = settings.gateway_host
FILESERVER_PORT = settings.gateway_port
METADATA_VALIDITY_DURATION = 60

MAX_CONCURRENT_BACKEND_CONNECTIONS = 100

FILE_METADATA_REQUEST_TIMEOUT = 30
FILE_LISTDIR_REQUEST_TIMEOUT = 45

logger = logging.getLogger(__name__)


def json_response(obj):
    obj['status'] = 'Ok'
    return json.dumps(obj)


def json_error(e):
    obj = {
        'status': 'Ko',
        'error': e
    }
    return json.dumps(obj)


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


def make_backend_request(req_path, callback, **kwargs):
    kwargs.update({
        'validate_cert': True,
        'ca_certs': settings.ssl_ca,
        'client_cert': settings.ssl_cert,
        'client_key': settings.ssl_key,
    })

    http_client = AsyncHTTPClient()
    http_client.fetch('https://%s:%s%s' % (
        options.fileserver_address,
        options.fileserver_port,
        req_path,
    ), callback, **kwargs)


def make_backend_post_request(req_path, data, callback, timeout=20):
    body = urllib.urlencode(data)
    make_backend_request(
        req_path,
        callback,
        method='POST',
        headers=None,
        body=body,
        request_timeout=timeout,
    )


class StatusHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def get(self):
        ret = {
        }
        return ret


class FileMetadataHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def handle_response(self, response):
        if response.error:
            print "Error:", response.error
            if response.error.code != 599:
                return {'Error': response.body}, response.error.code
            else:
                response.rethrow()
        else:
            jd = json.loads(response.body)

            # Format and store the response in the cache
            set_cached_file_metadata(jd)

            jd['act'] = 'CACHE_MISS'

            return jd

    @tornado.web.asynchronous
    def post(self):
        param_path = self.get_argument('path')
        force_refresh_arg = self.get_argument('force_refresh', default='FALSE')
        logger.info(u'file_metadata\t%s' % param_path)

        if force_refresh_arg == 'TRUE':
            force_refresh = True
        else:
            force_refresh = False

        if force_refresh is False:
            # First, check if we have the data in cache
            v = get_cached_file_metadata(param_path)
            if v is not None:
                v['act'] = 'CACHE_HIT'
                self.write(json_response(v))
                self.finish()
                return

        post_data = {'path': param_path.encode('UTF-8')}
        make_backend_post_request(
            '/file_metadata.json',
            post_data,
            self.handle_response,
            timeout=FILE_METADATA_REQUEST_TIMEOUT
        )


class ListDirHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def handle_response(self, response):
        if response.error:
            print "Error:", response.error
            if response.error.code != 599:
                return {'Error': response.body}, response.error.code
            else:
                response.rethrow()
        else:
            jd = json.loads(response.body)

            # Format and store all the data in the cache
            set_cached_list_dir(jd)
            jd['act'] = 'CACHE_MISS'

            return jd

    @tornado.web.asynchronous
    def post(self):
        param_path = self.get_argument('path')
        force_refresh_arg = self.get_argument('force_refresh', default='FALSE')
        logger.info(u'list_dir\t%s' % param_path)

        if force_refresh_arg == 'TRUE':
            force_refresh = True
        else:
            force_refresh = False

        if force_refresh is False:
            # First, check if we have the data in cache
            v = get_cached_list_dir(param_path)
            if v is not None:
                v['act'] = 'CACHE_HIT'
                self.write(json_response(v))
                self.finish()
                return

        post_data = {'dir': param_path.encode('UTF-8')}
        make_backend_post_request(
            '/list_dir.json',
            post_data,
            self.handle_response,
            timeout=FILE_LISTDIR_REQUEST_TIMEOUT,
        )


def tornado_app():
    twa = tornado.web.Application([
        (r'^/status.json$', StatusHandler),
        (r'^/list_dir.json$', ListDirHandler),
        (r'^/file_metadata.json$', FileMetadataHandler),
    ])

    return twa

if __name__ == '__main__':
    define('fileserver_address', default=FILESERVER_HOST)
    define('fileserver_port', default=FILESERVER_PORT, type=int)

    define('max_concurrent_backend_connections', default=MAX_CONCURRENT_BACKEND_CONNECTIONS, type=int)

    parse_command_line()

    tornado.httpclient.AsyncHTTPClient.configure(
        "tornado.simple_httpclient.SimpleAsyncHTTPClient",
        max_clients=options.max_concurrent_backend_connections)

    application = tornado_app()

    application.listen(LISTEN_PORT)

    IOLoop.current().start()
