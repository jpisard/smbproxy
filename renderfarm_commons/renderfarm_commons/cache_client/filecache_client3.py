# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

"""Public interface for the client to the raw-nginx based cache server"""


import base64
import json
import logging
import os
import shutil
import platform
import sys

import redis
from twisted.internet import defer
from twisted.internet.error import ReactorNotRunning

import luna_commons

from twisted_client import create_agent, upload, download_with_tmp_files, reactor


class CacheClient3(object):

    plt = platform.system()
    if plt == 'Windows':
        BLANK_FILE = 'C:\\Luna\\blank'
        client_certs_dir = 'C:\\Luna\\swift_certs'
    elif plt == 'Linux':
        BLANK_FILE = '/tmp/blank'
        client_certs_dir = '/tmp/seekscale_swift_certs'
    else:
        raise RuntimeError('Unrecognized platform: %s' % plt)

    all_keys_metakey = 'renderfarm:cacheclient3:keyset'

    def __init__(self, redis_host='10.91.0.1', ssl_cert=None, ssl_key=None, ssl_ca=None, concurrency_level=None):
        self.redis_host = redis_host

        if ssl_cert is not None:
            self.cacheclient_cert = ssl_cert
        else:
            self.cacheclient_cert = os.path.join(self.client_certs_dir, 'client.crt')

        if ssl_key is not None:
            self.cacheclient_key = ssl_key
        else:
            self.cacheclient_key = os.path.join(self.client_certs_dir, 'client.key')

        if ssl_ca is not None:
            self.cacheclient_ca = ssl_ca
        else:
            self.cacheclient_ca = os.path.join(self.client_certs_dir, 'ca.crt')

        # Setup connections to redis
        self.redis = redis.StrictRedis(host=self.redis_host, port=6379, db=0)

        self.get_certs()

        self.http_agent = create_agent(self.cacheclient_ca, self.cacheclient_cert, self.cacheclient_key)

        if concurrency_level is not None:
            deferred_semaphore = defer.DeferredSemaphore(concurrency_level)
        else:
            deferred_semaphore = None

        self.http_agent.deferred_semaphore = deferred_semaphore

        self.log = logging.getLogger(__name__)

    def get_certs(self):
        luna_commons.create_dir(self.client_certs_dir)
        if not os.path.exists(self.cacheclient_cert):
            r = luna_commons.download(
                'http://10.91.0.1:16000/client.crt',
                self.cacheclient_cert,
                timeout=10,
            )
            if not r:
                raise RuntimeError(u'Unable to fetch client.crt. Cannot initialize CacheClient.')

        if not os.path.exists(self.cacheclient_key):
            r = luna_commons.download(
                'http://10.91.0.1:16000/client.key',
                self.cacheclient_key,
                timeout=10,
            )
            if not r:
                raise RuntimeError(u'Unable to fetch client.key. Cannot initialize CacheClient.')

        if not os.path.exists(self.cacheclient_ca):
            r = luna_commons.download(
                'http://10.91.0.1:16000/ca.crt',
                self.cacheclient_ca,
                timeout=10,
            )
            if not r:
                raise RuntimeError(u'Unable to fetch ca.crt. Cannot initialize CacheClient.')

    @classmethod
    def cleanup_certs(cls):
        try:
            shutil.rmtree(cls.client_certs_dir)
        except:
            logging.warning(u'Could not delete cacheclient client certificates directory')

    def get_all_keys(self):
        return self.redis.get(self.all_keys_metakey)

    def key_from_file(self, path):
        try:
            st = os.stat(path)
            key = self.key_from_metadata(path, st.st_size, st.st_mtime)
        except Exception:
            self.log.warning(u"Could not compute fingerprint for file %s" % path, exc_info=True)

            # In this case, we send a blank file
            with open(self.BLANK_FILE, 'wb') as _:
                pass
            return self.key_from_file(self.BLANK_FILE)
        return key

    def key_from_metadata(self, path, size, mtime):
        if type(path) is unicode:
            path = path.encode('UTF-8')
        key = 'renderfarm:cacheclient3:file:%s:%d:%d' % (base64.b64encode(path), size, mtime)
        return key

    def has_file(self, key):
        """Checks whether the file exists on the cache server.
        We blindly assume that the metadata in the redis DB is up to date."""
        stored_manifest = self.get_file_manifest(key)
        if stored_manifest is not None:
            return True
        else:
            return False

    def get_file_manifest(self, key):
        raw_manifest = self.redis.get(key)
        if raw_manifest is None:
            return None
        else:
            return json.loads(raw_manifest)

    def set_file_manifest(self, key, manifest):
        self.redis.set(key, json.dumps(manifest))
        self.redis.sadd(self.all_keys_metakey, key)

    def add_file(self, key, path):
        d = upload(path, agent=self.http_agent)

        obj = self

        def store_result(res):
            # Store the manifest in redis so we can retrieve the file later
            obj.set_file_manifest(key, res)
            obj.log.info("File %s stored under key %s." % (path, key))
        d.addCallback(store_result)

        def handleError(error):
            obj.log.error("An error occured while uploading the file: %s" % error.getTraceback())
            return error
        d.addErrback(handleError)

        return d

    def cache_file(self, f):
        # Normalize the path
        path = os.path.abspath(f)

        # Compute a key for the file
        key = self.key_from_file(path)

        if key is None:
            self.log.error("Could not get a key for file")
            return defer.fail(None)

        d = defer.succeed(None)

        if not self.has_file(key):
            d.addCallback(lambda x: self.add_file(key, path))

        def handleSuccess(_):
            manifest = self.get_file_manifest(key)
            if manifest is None:
                self.log.warn('Manifest for %s is still none after uploading file. Something is wrong.' % path)

            return {
                'path': path,
                'key': key,
            }

        d.addCallback(handleSuccess)

        return d

    def get_file(self, key, target_path, overwrite=True):
        # Ensure target_path an absolute path
        target_path = os.path.abspath(target_path)

        # Make sure the directory we download to exists
        luna_commons.create_dir(os.path.dirname(target_path))

        if overwrite is False and os.path.exists(target_path):
            return

        # Get the manifest
        manifest = self.get_file_manifest(key)

        if manifest is None:
            return defer.fail(RuntimeError(u"Unknown key"))

        d = download_with_tmp_files(manifest, target_path, agent=self.http_agent)

        def handleError(error):
            self.log.error(u"An error occured while downloading the file: %s" % error.getTraceback())
            return error
        d.addErrback(handleError)

        return d

    def clear(self):
        """Resets the entire cache"""
        self.redis.flushall()
        # TODO: Add a way to remotely wipe the on-disk cache


def configure_parser(root_parser):
    """A generic command line submodule to interact with the cache.
    Example use:

    cacheclient_parser = main_subparsers.add_parser(
        'cacheclient',
        help='Interact with the file cache',
        description='Interact with the file cache',
    )
    renderfarm_commons.cache_client.filecache_client.configure_parser(cacheclient_parser)
    """


    subparsers = root_parser.add_subparsers(metavar='command')

    # Cache file command
    cache_file_parser = subparsers.add_parser(
        'cache-file',
        help='Uploads a file to the cache',
        description='Uploads a file to the cache',
    )
    cache_file_parser.add_argument('path')

    def run_cache_file(args):
        c = CacheClient3()
        d = c.cache_file(args.path)

        def handleResult(ret):
            print "File stored under key:", ret['key']
        d.addCallback(handleResult)

        def cbShutdown(_):
            try:
                reactor.stop()
            except ReactorNotRunning:
                pass
            sys.exit(0)
        d.addBoth(cbShutdown)

        reactor.run()

    cache_file_parser.set_defaults(func=run_cache_file)

    # Retrieve file command
    retrieve_file_parser = subparsers.add_parser(
        'retrieve-file',
        help='Downloads a file from the cache',
        description='Downloads a file from the cache',
    )
    retrieve_file_parser.add_argument('key')
    retrieve_file_parser.add_argument('path')

    def run_retrieve_file(args):
        c = CacheClient3()
        d = c.get_file(args.key, args.path, overwrite=True)

        def cbShutdown(_):
            try:
                reactor.stop()
            except ReactorNotRunning:
                pass
            sys.exit(0)
        d.addBoth(cbShutdown)

        reactor.run()

    retrieve_file_parser.set_defaults(func=run_retrieve_file)