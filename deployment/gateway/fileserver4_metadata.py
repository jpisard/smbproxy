# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""This version of the file server doesn't directly serve files. Instead, it does
a multipart upload wherever the client asks.

All HTTP endpoints expect parameters (and especially file paths) to be UTF-8 encoded.
"""

import base64
from functools import wraps
import json
import locale
import logging
import ntpath
import os
import stat
import sys
import traceback
import zlib

import redis

# Tornado-related stuff

from tornado.ioloop import IOLoop
from tornado.options import parse_command_line
import tornado.web
import tornado.gen

# import ujson

from fileserver4_path_helpers import translate_path, normalize_case, listdir
import settings

VERSION = u'fileserver-4'

MAX_WORKERS = 4

logger = logging.getLogger(__name__)

import luna_commons


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
        except Exception, e:
            logger.exception('Exception while processing response')
            exc_string = traceback.format_exc().decode(locale.getpreferredencoding())
            s.set_status(500)
            s.write(json_error(exc_string))
            s.finish()
            return
    return inner


def get_redis_conn():
    return redis.StrictRedis()


class StatusHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def get(self):
        ret = {
            u"version": VERSION,
        }
        return ret


def get_file_metadata(path, requested_path, listdir_cache, redis_conn):
    """
    Gets metadata for a given file
    :param path: the local path of the file (as unicode)
    :param requested_path: the network path of the file (as unicode)
    :param listdir_cache: a cache of listdir
    :return: a structure containing all the metadata for the file
    """

    key = 'file_metadata:' + base64.b64encode(path.encode('UTF-8'))
    r = redis_conn
    try:
        if not hasattr(r, 'disabled'):
            existing_val = r.get(key)
        else:
            existing_val = None
    except Exception:
        logger.warn("Warning: could not get key from redis: %s" % traceback.format_exc())
        existing_val = None
        r.disabled = True
    if existing_val is not None:
        ret = json.loads(existing_val)
        return ret

    else:
        ret = {'path': requested_path}
        rep = {}

        try:
            st = os.stat(path)
            ret['exists'] = True

            rep['normalized_path'] = normalize_case(requested_path, listdir_cache)

            rep['isdir'] = stat.S_ISDIR(st.st_mode)
            rep['isfile'] = stat.S_ISREG(st.st_mode)

            rep['st_mode'] = st.st_mode
            rep['st_ino'] = st.st_ino
            rep['st_dev'] = st.st_dev
            rep['st_nlink'] = st.st_nlink
            rep['st_uid'] = st.st_uid
            rep['st_gid'] = st.st_gid
            rep['st_size'] = st.st_size
            rep['st_atime'] = st.st_atime
            rep['st_mtime'] = st.st_mtime
            rep['st_ctime'] = st.st_ctime
        except os.error:
            ret['exists'] = False

        ret['metadata'] = rep

        try:
            if not hasattr(r, 'disabled'):
                r.setex(key, settings.file_metadata_cache_duration, json.dumps(ret))
        except Exception:
            logger.warn("Warning: could not set key in redis: %s" % traceback.format_exc())
            r.disabled = True

        return ret


class ListDirHandler(tornado.web.RequestHandler):
    def get_data(self, param_dir, root_dir):
        if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
            return {u'Error': u'Request to list_dir for a path that is not a directory'}, 400

        redis_conn = self.application.redis
        key = 'listdir:' + base64.b64encode(root_dir.encode('UTF-8'))
        try:
            if not hasattr(redis_conn, 'disabled'):
                existing_val = redis_conn.get(key)
            else:
                existing_val = None
        except Exception:
            logger.warn("Warning: could not get key from redis: %s" % traceback.format_exc())
            existing_val = None
            redis_conn.disabled = True
        if existing_val is not None:
            ret = json.loads(zlib.decompress(existing_val))
            return ret

        else:
            rep = listdir(root_dir)

            files_metadata = {}
            listdir_cache = {}
            for p in rep:
                files_metadata[p] = get_file_metadata(
                    os.path.join(root_dir, p),
                    ntpath.join(param_dir, p),
                    listdir_cache,
                    redis_conn,
                )
            total_size = 0
            for p in files_metadata:
                try:
                    if files_metadata[p]['metadata']['isfile']:
                        total_size += files_metadata[p]['metadata']['st_size']
                except KeyError:
                    logger.info(u"File \"%s\" in returned by listdir, but doesn't actually exist." % p)
            ret = {
                'directory': param_dir,
                'files': rep,
                'files_metadata': files_metadata,
                'total_size': total_size,
            }

            try:
                if not hasattr(redis_conn, 'disabled'):
                    redis_conn.setex(key, settings.listdir_cache_duration, zlib.compress(json.dumps(ret)))
            except Exception:
                logger.warn("Warning: could not set key in redis: %s" % traceback.format_exc())
                redis_conn.disabled = True

            return ret

    @tornado_json_endpoint
    def post(self):
        param_dir = self.get_argument('dir')
        root_dir = translate_path(param_dir)
        logger.info("list_dir\t%s" % root_dir)

        return self.get_data(param_dir, root_dir)


class FileMetadataHandler(tornado.web.RequestHandler):
    def get_data(self, path, param_path):
        listdir_cache = {}

        redis_conn = self.application.redis
        return get_file_metadata(
            path,
            param_path,
            listdir_cache,
            redis_conn
        )

    @tornado_json_endpoint
    def post(self):
        param_path = self.get_argument('path')
        path = translate_path(param_path)
        logger.info(u"Request for metadata of \"%s\"" % path)

        return self.get_data(path, param_path)


class DeleteHandler(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def post(self):
        param_path = self.get_argument('file')
        path = translate_path(param_path)
        logger.info(u"Request for deletion of \"%s\"" % path)

        if not os.path.exists(path):
            logger.info(u"File doesn't exist. Doing nothing.")
            return {}
        else:
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except:
                    if os.path.exists(path):
                        logger.info(u"File deletion failed and file still exists. This is an error.")
                        raise
                    else:
                        logger.info(u"File deletion failed, but file is no longer present. Assuming success.")

                r = self.application.redis
                key = 'file_metadata:' + base64.b64encode(path.encode('UTF-8'))
                try:
                    if not hasattr(r, 'disabled'):
                        r.delete(key)
                except Exception:
                    logger.warn('Could not delete key in redis: %s' % traceback.format_exc())
                    r.disabled = True

                file_dir = os.path.dirname(path)
                dir_key = 'listdir:' + base64.b64encode(file_dir.encode('UTF-8'))
                try:
                    if not hasattr(r, 'disabled'):
                        r.delete(dir_key)
                except Exception:
                    logger.warn('Could not delete key in redis: %s' % traceback.format_exc())
                    r.disabled = True

                return {}
            else:
                logger.info(u"Trying to delete something that isn't a file. This is unsupported")
                return {u'Error': u"Trying to delete something that isn't a file. This is unsupported"}, 400


class TouchFile(tornado.web.RequestHandler):
    @tornado_json_endpoint
    def post(self):
        param_path = self.get_argument('file')
        path = translate_path(param_path)
        logger.info(u"Request to touch \"%s\"" % path)

        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            luna_commons.create_dir(dirname)

        if not os.path.exists(path):
            with open(path, 'wb') as _:
                pass

        r = self.application.redis
        key = 'file_metadata:' + base64.b64encode(path.encode('UTF-8'))
        try:
            if not hasattr(r, 'disabled'):
                r.delete(key)
        except Exception:
            logger.warn('Warning: could not delete key from redis: %s' % traceback.format_exc())
            r.disabled = True

        file_dir = os.path.dirname(path)
        dir_key = 'listdir:' + base64.b64encode(file_dir.encode('UTF-8'))
        try:
            if not hasattr(r, 'disabled'):
                r.delete(dir_key)
        except Exception:
            logger.warn('Warning: could not delete key from redis: %s' % traceback.format_exc())
            r.disabled = True

        return {}


class PutFileHandler(tornado.web.RequestHandler):
    def get_data(self, path, f):
        if f is None or len(f) == 0:
            raise RuntimeError("No file uploaded.")

        with open(path, 'wb') as fh:
            fh.write(f[0].body)

        r = self.application.redis
        key = 'file_metadata:' + base64.b64encode(path.encode('UTF-8'))
        try:
            if not hasattr(r, 'disabled'):
                r.delete(key)
        except Exception:
            logger.warn('Warning: could not delete key from redis: %s' % traceback.format_exc())
            r.disabled = True

        file_dir = os.path.dirname(path)
        dir_key = 'listdir:' + base64.b64encode(file_dir.encode('UTF-8'))
        try:
            if not hasattr(r, 'disabled'):
                r.delete(dir_key)
        except Exception:
            logger.warn('Warning: could not delete key from redis: %s' % traceback.format_exc())
            r.disabled = True

        file_size = os.path.getsize(path)
        ret = {
            'path': path,
            'file_size': file_size
        }

        return ret

    @tornado_json_endpoint
    def post(self):
        param_path = self.get_argument('path')
        path = translate_path(param_path)
        logger.info(u"Request to write \"%s\"" % path)

        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            luna_commons.create_dir(dirname)

        f = self.request.files.get('file')

        return self.get_data(path, f)


class GetFile(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        param_path = self.get_argument('file')
        path = translate_path(param_path)
        logger.info(u"Request for \"%s\"" % path)
        try:
            with open(path, 'rb') as f:
                while 1:
                    data = f.read(16384)  # or some other nice-sized chunk
                    if not data:
                        break
                    self.write(data)
                    yield tornado.gen.Task(self.flush)
            self.finish()
        except IOError:
            raise tornado.web.HTTPError(404, 'Invalid file')


def tornado_app():
    twa = tornado.web.Application([
        (r'^/status.json$', StatusHandler),
        (r'^/list_dir.json$', ListDirHandler),
        (r'^/file_metadata.json$', FileMetadataHandler),
        (r'^/delete_file.json$', DeleteHandler),
        (r'^/put$', PutFileHandler),
        (r'^/get$', GetFile),
        (r'^/touch_file.json$', TouchFile),
    ])

    twa.cache_client = None
    twa.redis = get_redis_conn()

    return twa


if __name__ == "__main__":
    parse_command_line()

    logger.info(u"Environment is %s" % repr(os.environ))
    logger.info(u"Filesystem encoding: %s" % sys.getfilesystemencoding())

    application = tornado_app()

    if len(sys.argv) >= 2:
        listen_port = int(sys.argv[1])
    else:
        listen_port = 15024
    application.listen(listen_port)

    IOLoop.current().start()