# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import base64
from datetime import datetime
import json
import ntpath
import os
import random
import tempfile
import traceback
import uuid

import redis
import requests
import requests.exceptions
from twisted.internet import defer, reactor
from twisted.python import failure
import treq

from seekscale_commons.cache_client import filecache_client3

import logger
from statsd_logging import StatsdClient
from metadata_proxy import metadata_loader
from ssl_agent import create_agent


def get_traceback():
    f = failure.Failure()
    return f.getTraceback()


def sleep(seconds):
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, seconds)
    return d


class FSCacheFileMetadata(object):
    def __init__(self, share_name, path, metadata, log):
        self.share_name = share_name
        self.path = path
        self.metadata = metadata
        self.log = log
        self._children_metadata = None

    def set_children(self, children_metadata):
        self._children_metadata = children_metadata

    def exists(self):
        """Tests whether the file exists"""
        # TODO: FSCache.exists() has an "optimized" version that does a completely different test. Is it still useful?
        if self.metadata is not None:
            return self.metadata['exists']
        else:
            return False

    def is_file(self):
        """Tests whether f points to a file on the asset server"""
        if self.metadata is not None:
            try:
                return self.metadata['metadata']['isfile']
            except KeyError:
                return False
        else:
            return False

    def is_dir(self):
        """Tests whether f points to a directory on the asset server"""
        if self.metadata is not None:
            try:
                return self.metadata['metadata']['isdir']
            except KeyError:
                return False
        else:
            return False

    def children_metadata(self):
        """Returns a list of FSCacheFileMetadata for the children files"""
        if self._children_metadata is not None:
            return self._children_metadata
        else:
            self.log.msg("WARNING: Requested children_metadata before is was set. Returning [].", level=logger.WARN)
            return []

    def has_normalized_path(self):
        if self.metadata is not None and self.metadata['metadata'].get('normalized_path', None) is not None:
            return True
        else:
            return False

    def normalized_path(self):
        """Returns the path of a file with correct studio-side case"""
        if self.metadata is not None:
            v = self.metadata['metadata'].get('normalized_path', None)
            if v is not None:
                return v
            else:
                self.log.msg(
                    "WARNING: normalized_path is None for %s:%s" % (self.share_name, self.path), level=logger.INFO
                )
                return self.path
        else:
            return self.path

    def mtime(self):
        """Returns the mtime of a file on the asset server"""
        if self.metadata is not None:
            return self.metadata['metadata']['st_mtime']
        else:
            return None

    def size(self):
        """Returns the size of a file on the asset server"""
        if self.metadata is not None:
            return self.metadata['metadata']['st_size']
        else:
            return 0

    def parent_metadata(self):
        """Returns the metadata of the parent file"""
        path = ntpath.normpath(self.path)
        parent_path = ntpath.dirname(path)

        normalized_path = ntpath.normpath(self.normalized_path())
        normalized_parent_path = ntpath.dirname(normalized_path)

        if path != parent_path and parent_path != '':
            parent_data = {
                'metadata': {
                    'exists': True,
                    'isdir': True,
                    'normalized_path': normalized_parent_path,
                }
            }
            parent_share = self.share_name
            parent_path = parent_path
            return FSCacheFileMetadata(parent_share, parent_path, parent_data, self.log)
        else:
            return None


class FSCacheHTTPConnector(object):
    """
    The module that handles all HTTP connections to backend and metadata servers
    """

    # We use a semaphore to limit the number of concurrent outbound connections.
    sem = defer.DeferredSemaphore(15)

    # Telemetry counters
    requests_stats = {
        'pending': 0,
        'success': 0,
        'failure': 0,
        'total_failure': 0,

        'cache_client.write.pending': 0,
        'cache_client.write.success': 0,
        'cache_client.write.failure': 0,

        'cache_client.read.pending': 0,
        'cache_client.read.success': 0,
        'cache_client.read.failure': 0,

        'operation.failure': 0,
    }
    stats_client = StatsdClient.get()

    def __init__(
            self,
            http_service_host,
            http_service_port,
            metadata_proxy_host,
            metadata_proxy_port,
            log, settings,
            tmpdir,
            cache_client,
            redis_host='127.0.0.1'
    ):
        self.http_service_host = http_service_host
        self.http_service_port = http_service_port
        self.metadata_proxy_host = metadata_proxy_host
        self.metadata_proxy_port = metadata_proxy_port
        self.log = log
        self.settings = settings
        self.TMPDIR = tmpdir
        self.cache_client = cache_client

        self.redis_host = redis_host
        self.redis = redis.StrictRedis(host=self.redis_host, port=6379, db=0)

        self.agent = create_agent(
            settings.ssl_ca,
            settings.ssl_cert,
            settings.ssl_key
        )

    @classmethod
    def _get_semaphore(cls):
        return cls.sem

    #
    # Telemetry helpers
    #
    @classmethod
    def incr_pending_requests_count(cls):
        cls.requests_stats['pending'] += 1
        cls.stats_client.gauge('http.requests.pending', cls.requests_stats['pending'])

    @classmethod
    def decr_pending_requests_count(cls):
        cls.requests_stats['pending'] -= 1
        cls.stats_client.gauge('http.requests.pending', cls.requests_stats['pending'])

    @classmethod
    def pending_requests_count(cls):
        return cls.requests_stats['pending']

    @classmethod
    def incr_counter(cls, counter_name):
        cls.requests_stats[counter_name] += 1

    @classmethod
    def get_counter(cls, counter_name):
        return cls.requests_stats[counter_name]

    @classmethod
    def decr_pending_cacheclient_write_requests_count(cls):
        cls.requests_stats['cache_client.write.pending'] -= 1

    @classmethod
    def decr_pending_cacheclient_read_requests_count(cls):
        cls.requests_stats['cache_client.read.pending'] -= 1

    def register_operation_failure(self, v):
        self.incr_counter('operation.failure')
        return v

    #
    # The raw HTTP requests
    #

    # def _http_req(self, endpoint, **kwargs):
    #     url = 'http://%s:%d/%s' % (self.http_service_host, self.http_service_port, endpoint)
    #
    #     # self.log.msg("[HTTP] %s %r" % (url, kwargs), level=logger.INFO)
    #
    #     try:
    #         r = requests.post(url, **kwargs)
    #     except requests.exceptions.RequestException:
    #         self.log.msg("ERROR in HTTP request: %s" % get_traceback(), level=logger.INFO)
    #     else:
    #         if r.status_code != 200:
    #             self.log.msg("Bad status code in HTTP request: %d" % r.status_code, level=logger.INFO)
    #             return None
    #         else:
    #             return r

    def _http_treq_req_metadata(self, endpoint, log=None, **kwargs):
        """
        Makes a POST request to the metadata proxy.
        :param endpoint: the URL we want to hit
        :param log: (optional) a logging context
        :param kwargs: additional parameters to be passed to the treq.post call
        :return: a Deferred that fires the content of the HTTP response
        """
        return self._http_treq_req_base(endpoint, self.metadata_proxy_host, self.metadata_proxy_port, log, False, **kwargs)

    def _http_treq_req(self, endpoint, log=None, **kwargs):
        """
        Makes a POST request to the backend fileserver.
        :param endpoint: the URL we want to hit
        :param log: (optional) a logging context
        :param kwargs: additional parameters to be passed to the treq.post call
        :return: a Deferred that fires the content of the HTTP response
        """
        return self._http_treq_req_base(endpoint, self.http_service_host, self.http_service_port, log, True, **kwargs)

    @defer.inlineCallbacks
    def _http_treq_req_base(self, endpoint, host, port, log=None, ssl=False, **kwargs):
        """
        Makes a POST request to a backend server.
        :param endpoint: the URL we want to hit
        :param host: the remote host
        :param port: the remove port
        :param log: (optional) a logging context
        :param kwargs: additional parameters to be passed to the treq.post call
        :return: a Deferred that fires the content of the HTTP response
        """
        if log is None:
            log = self.log
        log = log.bind(
            http_request_id=str(uuid.uuid4()),
        )
        if ssl:
            url = 'https://%s:%d/%s' % (host, port, endpoint)
        else:
            url = 'http://%s:%d/%s' % (host, port, endpoint)

        # log.msg("[HTTP ASYNC] %s %r" % (url, kwargs), level=logger.INFO)
        self.stats_client.incr('http.requests.started')

        start_timestamp = datetime.now()
        # treq doesn't seem to handle unicode parameters in data very well.
        # Manually encode to UTF-8
        if 'data' in kwargs:
            for k in kwargs['data']:
                if type(kwargs['data'][k]) is unicode:
                    kwargs['data'][k] = kwargs['data'][k].encode('UTF-8')

        self.incr_pending_requests_count()

        @defer.inlineCallbacks
        def make_request():
            if ssl:
                response = yield treq.post(url, agent=self.agent, **kwargs)
            else:
                response = yield treq.post(url, **kwargs)

            try:
                content = yield treq.content(response)
            except:
                log.msg("Error while reading body in HTTP response",
                        level=logger.WARN)
                self.stats_client.incr('http.requests.errors.read_body_error')
                err = RuntimeError("Error while reading body in HTTP response (Response code: %d)." % response.code)
                err.status_code = response.code
                raise err
            else:
                if response.code == 200:
                    defer.returnValue(content)
                else:
                    log.msg("Error: Bad status code in HTTP response",
                            http_response_code=response.code,
                            http_content=content,
                            level=logger.WARN)
                    self.stats_client.incr('http.requests.errors.bad_status_code.%d' % response.code)
                    err = RuntimeError('Error: Bad status code in HTTP response: %d' % response.code)
                    err.status_code = response.code
                    raise err

        try:
            # Run on the semaphore, to limit the number of concurrent HTTP connections
            rep = yield self._get_semaphore().run(make_request)
        except Exception, e:
            end_timestamp = datetime.now()
            self.incr_counter('failure')
            self.stats_client.incr('http.requests.failed')
            ms = (end_timestamp-start_timestamp).total_seconds()*1000
            log.msg("HTTP Request failed in %sms" % ms,
                    http_request_length=ms,
                    level=logger.WARN,
                    error=get_traceback(),
                    )
            self.decr_pending_requests_count()
            raise e
        else:
            end_timestamp = datetime.now()
            self.incr_counter('success')
            self.stats_client.incr('http.requests.succeeded')
            ms = (end_timestamp-start_timestamp).total_seconds()*1000
            # log.msg("Operation completed in %sms" % ms, http_request_length=ms, level=logger.INFO)
            self.stats_client.timing('http.requests.duration', int(ms))
            self.decr_pending_requests_count()

            defer.returnValue(rep)

    def _http_treq_req_with_retry_metadata(self, endpoint, req_timeout=10, **kwargs):
        return self._http_treq_req_with_retry_base(
                endpoint, self.metadata_proxy_host, self.metadata_proxy_port, ssl=False, req_timeout=req_timeout, **kwargs)

    def _http_treq_req_with_retry(self, endpoint, req_timeout=10, **kwargs):
        return self._http_treq_req_with_retry_base(
                endpoint, self.http_service_host, self.http_service_port, ssl=True, req_timeout=req_timeout, **kwargs)

    @defer.inlineCallbacks
    def _http_treq_req_with_retry_base(self, endpoint, host, port, ssl=False, req_timeout=10, **kwargs):
        """Make a request to the fileserver4. Retries it on error, after an increasing waiting time.
        Returns a Deferred that fires the content of the HTTP response.
        Or error, fire a RuntimeError."""
        log = self.log.bind(
            http_request_type="async",
            http_request_series_id=str(uuid.uuid4()),
            host=host,
            port=port,
            http_endpoint=endpoint
        )

        # Increasing delays before retrying connections
        retry_delays = [0, 2, 3, 5, 15, 30, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60]
        max_attempts = len(retry_delays)

        def wait(timeout):
            d = defer.Deferred()
            reactor.callLater(timeout, d.callback, None)
            return d

        kwargs.update(timeout=req_timeout)

        last_status_code = None

        for attempt in range(max_attempts):
            try:
                # The actual delay in randomize within 0.75-1.25 times the hardcoded, to avoid thundering herd issues
                retry_delay = ((random.random()*0.5)+0.75) * retry_delays[attempt]
                yield wait(retry_delay)
                r = yield self._http_treq_req_base(endpoint, host, port, log=log, ssl=ssl, **kwargs)
                defer.returnValue(r)
            except Exception, e:
                self.log.msg("Error: %s" % get_traceback())

                # If we got a HTTP response code different from 500, there is no point in retrying.
                if hasattr(e, 'status_code'):
                    last_status_code = e.status_code
                    if last_status_code != 500:
                        break

        self.incr_counter('total_failure')
        self.stats_client.incr('http.requests.given_up')

        err = RuntimeError('Gave up trying after too many HTTP requests failures')
        err.status_code = last_status_code
        raise err

    #
    # Public API
    #
    @defer.inlineCallbacks
    def http_get_metadata_async(self, full_path, force_refresh=False):
        """
        Queries the fileserver for metadata of a given path. Fires the content of the fileserver response.
        :param full_path:
        :param force_refresh:
        :return:
        """
        data = {'path': full_path}

        if force_refresh:
            data['force_refresh'] = 'TRUE'

        try:
            rep = yield self._http_treq_req_with_retry_metadata('file_metadata.json', data=data)
            rj = json.loads(rep)
            defer.returnValue(rj)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_get_dirlist_async(self, full_path, force_refresh=False):
        """
        Queries the contents of a remote directory.
        :param full_path: the requested path
        :param force_refresh: whether we want to force a check on the remote server
        :return: a Deferred that fires the content of the fileserver response
        """
        data = {'path': full_path}

        if force_refresh:
            data['force_refresh'] = 'TRUE'

        try:
            rep = yield self._http_treq_req_with_retry_metadata(
                    'list_dir.json', req_timeout=self.settings.LIST_DIR_TIMEOUT, data=data)
            rj = json.loads(rep)
            defer.returnValue(rj)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_get_file_async(self, full_path):
        """
        Gets a file directly through the fileserver.
        :param full_path: the requested path
        :return: a Deferred that fires a temporary path containing the file
        """
        fd = tempfile.NamedTemporaryFile(dir=self.TMPDIR, delete=False)

        data = {'file': full_path}

        try:
            data = yield self._http_treq_req_with_retry('get', req_timeout=60, data=data)
            fd.write(data)
            fd.close()
            defer.returnValue(fd.name)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_get_file_cacheclient3(self, full_path):
        """
        Pulls a file through CacheClient3
        :param full_path: the requested path
        :return: a Deferred that fires the key where the file has been uploaded
        """
        data = {'path': full_path}

        try:
            rep = yield self._http_treq_req('cache_file3.json', data=data)
            rj = json.loads(rep)
            key = rj['key']
            defer.returnValue(key)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_write_file_queue(self, full_path, local_path):
        timeout = 1200

        cache = self.cache_client

        self.incr_counter('cache_client.write.pending')
        try:
            cache_file_result = yield cache.cache_file(local_path)
            self.decr_pending_cacheclient_write_requests_count()
        except Exception:
            self.decr_pending_cacheclient_write_requests_count()
            raise
        else:
            try:
                if cache_file_result is None:
                    # Caching failed
                    err = RuntimeError('Caching failed in write_file_queue for file %s' % local_path)
                    raise err
                else:
                    jid = 'bkgrd_dl:job:%s' % str(uuid.uuid4())
                    file_path = full_path
                    file_key = cache_file_result['key']
                    self.redis.hmset(jid, {
                        'path': file_path,
                        'key': file_key,
                    })

                    self.redis.lpush('bkgrd_dl:pending', jid)

                    final_status = False
                    for _ in range(timeout):
                        # Check if the job is marked as done
                        v = self.redis.hget(jid, 'state')
                        if v is None:
                            pass
                        elif v == 'SUCCESS':
                            final_status = True
                            break
                        elif v == 'FAILURE':
                            err = RuntimeError('File transfer failed for file %s' % local_path)
                            raise err

                        yield sleep(1.0)

                if final_status is False:
                    err = RuntimeError('File transfer timed out for file %s' % local_path)
                    raise err

                self.incr_counter('cache_client.write.success')
                defer.returnValue(True)

            except Exception, e:
                self.incr_counter('cache_client.write.failure')
                self.register_operation_failure(e)
                raise

    @defer.inlineCallbacks
    def http_get_file_through_cacheclient3(self, full_path, size, mtime):
        cache = self.cache_client

        unverified_key = cache.key_from_metadata(full_path, size, mtime)

        # If we don't have anything, make a call to fileserver to upload the file to the cache
        if not cache.has_file(unverified_key):
            try:
                file_key = yield self.http_get_file_cacheclient3(full_path)
            except Exception, e:
                self.register_operation_failure(e)
                raise
        else:
            file_key = unverified_key

        # Download the file from the cache to a temporary file, and return it
        tmp = tempfile.NamedTemporaryFile(dir=self.TMPDIR, delete=False)
        tmp.close()

        self.incr_counter('cache_client.read.pending')
        try:
            yield cache.get_file(file_key, tmp.name, overwrite=True)
            self.decr_pending_cacheclient_read_requests_count()
            self.incr_counter('cache_client.read.success')
        except Exception, e:
            self.decr_pending_cacheclient_read_requests_count()
            self.incr_counter('cache_client.read.failure')
            self.register_operation_failure(e)
            err = RuntimeError('Could not get file from CacheClient3: %s', get_traceback())
            raise err
        else:
            defer.returnValue(tmp.name)

    @defer.inlineCallbacks
    def http_write_file_async(self, full_path, local_path):
        """
        Writes a local file directly through the fileserver.
        :param full_path: the backend path where the file will get written
        :param local_path: the local path to read
        :return: True if the operation succeeded, pass the exception if it didn't
        """
        data = {'path': full_path}
        files = {'file': open(local_path, 'rb')}

        try:
            yield self._http_treq_req_with_retry('put', req_timeout=60, data=data, files=files)
            defer.returnValue(True)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_delete_file_async(self, full_path):
        """
        Deletes a file from the studio fileserver.
        :param full_path: the backend path to delete
        :return: True if the operation succeeded, pass the exception if it didn't
        """
        data = {'file': full_path}
        try:
            yield self._http_treq_req_with_retry('delete_file.json', data=data)
            defer.returnValue(True)
        except Exception, e:
            self.register_operation_failure(e)
            raise

    @defer.inlineCallbacks
    def http_touch_file_async(self, full_path):
        """
        Touches a file on the studio fileserver.
        :param full_path:
        :return: True if the operation succeeded, pass the exception if it didn't
        """
        data = {'file': full_path}
        try:
            yield self._http_treq_req_with_retry('touch_file.json', data=data)
            defer.returnValue(True)
        except Exception, e:
            self.register_operation_failure(e)
            raise


class FSCache(object):
    # This might be tweaked later if we want to centralize things in a bigger unified infrastructure.
    CLUSTER_ID = "0"

    def __init__(
            self,
            settings,
            host='gateway.seekscale.com',
            port=15024,
            metadata_proxy_host='10.91.1.254',
            metadata_proxy_port=25200,
            redis_host='127.0.0.1'
    ):
        self.next_id = 0
        self.metadata_cache = {}

        self.http_connector_host = host
        self.http_connector_port = port

        self.metadata_proxy_host = metadata_proxy_host
        self.metadata_proxy_port = metadata_proxy_port

        self.settings = settings
        self.TMPDIR = os.path.join(self.settings.SHARES_ROOT, '.seekscale_tmp')

        self.redis_host = redis_host

        self.cache_host = settings.cache_host
        self.ssl_cert = settings.ssl_cert
        self.ssl_key = settings.ssl_key
        self.ssl_ca = settings.ssl_ca

        try:
            self.cache_client = filecache_client3.CacheClient3(
                redis_host=self.cache_host,
                ssl_cert=self.ssl_cert,
                ssl_key=self.ssl_key,
                ssl_ca=self.ssl_ca,
                concurrency_level=15
            )
        except Exception:
            log = logger.logger.new()
            log.msg('Cannot initialize cache_client. Working without it.', level=logger.WARN)
            self.cache_client = None

    def get_http_connector(self, log):
        return FSCacheHTTPConnector(
            self.http_connector_host,
            self.http_connector_port,
            self.metadata_proxy_host,
            self.metadata_proxy_port,
            log,
            self.settings,
            self.TMPDIR,
            self.cache_client,
            redis_host=self.redis_host)

    @staticmethod
    def full_path_from_sharename(share_name, path):
        # If full_share is a drive, map it back to a real drive
        (_1, _2, unc_host, unc_share) = share_name.split('\\')
        if unc_host.startswith('hostluna_drive_'):
            drive_letter = unc_host.split('_')[2]
            path_prefix = drive_letter + ':\\'
            full_filename = ntpath.join(path_prefix, path)
        elif unc_host.startswith('hostluna_nfs'):
            path_prefix = base64.b64decode(unc_share)
            path_suffix = path.replace('\\', '/')
            full_filename = os.path.join(path_prefix, path_suffix)
        else:
            full_filename = ntpath.join(share_name, path)

        return full_filename

    #
    # The convenience functions that provide caching of everything
    #
    def metadata_valid(self, share_name, path):
        if (share_name, path,) not in self.metadata_cache:
            return False
        else:
            metadata_validity_duration = self.get_metadata_max_age_for_path(share_name, path)

            if (datetime.utcnow() - self.metadata_cache[(share_name, path,)]['_last_check_date']).total_seconds() > \
                    metadata_validity_duration:
                return False
            else:
                return True

    def get_metadata_max_age_for_path(self, share_name, path):
        metadata_validity_duration = self.settings.MTIME_METADATA_REFRESH_THRESHOLD

        for static_pattern in self.settings.NO_RECHECK_METADATA_PATTERNS:
            full_path = self.full_path_from_sharename(share_name, path)
            if full_path.startswith(static_pattern):
                # "Static" paths have a validity duration of one day
                metadata_validity_duration = 24*3600
                break

        return metadata_validity_duration

    def set_metadata(self, share_name, path, data):
        self.metadata_cache[(share_name, path,)] = data
        self.metadata_cache[(share_name, path,)]['_last_check_date'] = datetime.utcnow()

    @defer.inlineCallbacks
    def get_metadata_async_old(self, share_name, path, log, force_update=False):
        """
        Retrieves metadata for a given path
        :param share_name: The requested share
        :param path: The requested path
        :param log: A log context
        :param force_update: Whether to force refreshing the metadata from the remote fileserver
        :return: The API reponse object from the fileserver
        """
        http_connector = self.get_http_connector(log)
        if force_update or not self.metadata_valid(share_name, path):
            full_path = self.full_path_from_sharename(share_name, path)
            rep = yield http_connector.http_get_metadata_async(full_path)
            self.set_metadata(share_name, path, rep)

        defer.returnValue(self.metadata_cache[(share_name, path,)])

    @defer.inlineCallbacks
    def get_metadata_async(self, share_name, path, log, force_update=False):
        """
        Retrieves metadata for a given path
        :param share_name: The requested share
        :param path: The requested path
        :param log: A log context
        :param force_update: Whether to force refreshing the metadata from the remote fileserver
        :return: The file metadata information from the metadata cache
        """

        full_path = self.full_path_from_sharename(share_name, path)
        max_age = self.get_metadata_max_age_for_path(share_name, path)

        if not force_update:
            # Check if we have the metadata in the local redis DB and if it is still valid
            v = metadata_loader.get_cached_file_metadata(full_path, max_age=max_age)
            if v is not None:
                defer.returnValue(v)

        # If invalid, escalate to the central entrypoint, which will proxy the request towards the gateway
        http_connector = self.get_http_connector(log)
        rep = yield http_connector.http_get_metadata_async(full_path, force_refresh=force_update)
        defer.returnValue(rep)

    @defer.inlineCallbacks
    def get_dir_listing_async_old(self, share_name, path, log, force_update=False):
        http_connector = self.get_http_connector(log)
        metadata = yield self.get_metadata_async(share_name, path, log)

        if force_update or ('files' not in metadata):
            full_path = self.full_path_from_sharename(share_name, path)

            dir_listing = yield http_connector.http_get_dirlist_async(full_path)

            if dir_listing is not None and 'files' in dir_listing:
                metadata['files'] = dir_listing['files']
            else:
                log.msg('Warning: No list of children files returned for get_dir_listing_async(%s, %s)' % (
                    share_name, path.encode('UTF-8')), level=logger.INFO)
                defer.returnValue([])

            # dir_listing also has the metadata for children files. Cache those too.
            if 'files_metadata' in dir_listing:
                for p in dir_listing['files_metadata']:
                    child_path = ntpath.join(path, p)
                    self.set_metadata(share_name, child_path, dir_listing['files_metadata'][p])

        defer.returnValue(metadata['files'])

    @defer.inlineCallbacks
    def get_dir_listing_async(self, share_name, path, log, force_update=False):
        http_connector = self.get_http_connector(log)
        max_age = self.get_metadata_max_age_for_path(share_name, path)
        full_path = self.full_path_from_sharename(share_name, path)

        dir_listing = None

        if not force_update:
            # Check if we have some valid data in the local redis DB
            v = metadata_loader.get_cached_list_dir(full_path, max_age=max_age)
            if v is not None:
                dir_listing = v

        if dir_listing is None:
            # Make a query on the metadata proxy
            dir_listing = yield http_connector.http_get_dirlist_async(full_path, force_refresh=force_update)

        if dir_listing is not None and 'files' in dir_listing:
            defer.returnValue((dir_listing['files'], dir_listing['files_metadata']))
        else:
            log.msg('Warning: No list of children files returned for get_dir_listing_async(%s, %s)' % (
                share_name, path.encode('UTF-8')), level=logger.WARN)
            defer.returnValue(([], {}))

    #
    # Public API
    #
    @defer.inlineCallbacks
    def get_file(self, file_metadata, log):
        """
        Retrieves a file from the data backend. Raises on error
        :param file_metadata:
        :param log: A log context
        :return: a deferred that yields a temporary path to the downloaded file
        """
        http_connector = self.get_http_connector(log)
        # Compute a key and lookup into the cache
        full_path = self.full_path_from_sharename(file_metadata.share_name, file_metadata.path)
        size = file_metadata.size()
        mtime = file_metadata.mtime()

        if size < self.settings.CACHECLIENT3_SIZE_THRESHOLD or self.cache_client is None:
            r = yield http_connector.http_get_file_async(full_path)
        else:
            r = yield http_connector.http_get_file_through_cacheclient3(full_path, size, mtime)

        defer.returnValue(r)

    @defer.inlineCallbacks
    def set_file(self, file_metadata, local_path, log):
        """
        Copies local_path to the path represented in file_metadata
        :param file_metadata:
        :param local_path: The local path of the file
        :param log: A log context
        :return:
        """
        http_connector = self.get_http_connector(log)
        full_path = self.full_path_from_sharename(file_metadata.share_name, file_metadata.path)

        if os.path.getsize(local_path) > self.settings.CACHECLIENT3_SIZE_THRESHOLD and self.cache_client is not None:
            r = yield http_connector.http_write_file_queue(full_path, local_path)
        else:
            r = yield http_connector.http_write_file_async(full_path, local_path)

        defer.returnValue(r)

        # FIXME: A sync immediately after the write will cause the file to be reimported. Adapt the old workaround:
        # #
        # # Manually update our local metadata cache
        # #
        #
        # # We get the written metadata. We assume that the distant file doesn't change between the moment we write
        # # the file and the moment we read it back.
        # metadata = self.get_metadata(share_name, path, force_update=True)
        # try:
        #     size = metadata['metadata']['st_size']
        #     mtime = metadata['metadata']['st_mtime']
        # except KeyError:
        #     info_print("\tget_metadata() returned nothing for file \"%s:%s\" which we just created. "
        #                "Either the write failed, or the file was deleted in the meantime." % (share_name, path))
        # else:
        #     r = redis.StrictRedis(host=self.REDIS_HOST, port=6379, db=0)
        #     key = self.key_from_file(full_path, size, mtime)
        #     r.set(key, checksum)

    @defer.inlineCallbacks
    def delete_file(self, share_name, path, log):
        """
        Delete a file.
        Fires True on success, raise an exception on failure.
        :param share_name:
        :param path:
        :param log:
        :return:
        """
        http_connector = self.get_http_connector(log)
        full_path = self.full_path_from_sharename(share_name, path)

        success = yield http_connector.http_delete_file_async(full_path)

        if success:
            #
            # Manually update our local metadata cache
            #
            npath = ntpath.normpath(path)
            parent_path = ntpath.dirname(npath)
            _, _ = yield self.get_dir_listing_async(share_name, parent_path, log, force_update=True)

        defer.returnValue(success)

    @defer.inlineCallbacks
    def touch_file(self, share_name, path, log):
        """
        Touches a file.
        Fires True on success, raise an exception on failure.
        :param share_name:
        :param path:
        :param log:
        :return:
        """
        http_connector = self.get_http_connector(log)
        full_path = self.full_path_from_sharename(share_name, path)

        success = yield http_connector.http_touch_file_async(full_path)

        defer.returnValue(success)

    @defer.inlineCallbacks
    def metadata_object(self, share_name, path, log, include_children=True):
        """
        Returns a FSCacheFileMetadata object containing all the information required about this file
        :param share_name:
        :param path:
        :param log:
        :param include_children:
        :return:
        """
        metadata = yield self.get_metadata_async(share_name, path, log)

        processed_metadata = FSCacheFileMetadata(share_name, path, metadata, log)

        # For a directory, add the metadata of children files
        if include_children and processed_metadata.is_dir():
            child_files, children_metadata = yield self.get_dir_listing_async(share_name, path, log)

            # children_metadata_deferreds = [
            #     self.metadata_object(share_name, ntpath.join(path, child_file), log, include_children=False)
            #     for child_file in child_files
            # ]
            #
            # r = yield defer.DeferredList(children_metadata_deferreds, consumeErrors=True)
            # children_metadata = [data for (result, data) in r]

            processed_children_metadata = [
                FSCacheFileMetadata(
                    share_name,
                    ntpath.join(path, child_file),
                    children_metadata[child_file],
                    log
                )
                for child_file in child_files
            ]

            processed_metadata.set_children(processed_children_metadata)

        defer.returnValue(processed_metadata)

    def flush_metadata_cache(self):
        self.metadata_cache = {}

    # Deprecated, but kept for the idea. This is optimization hasn't been reimplemented, I'm not sure if it is still
    # relevant
    def exists(self, share_name, path):
        """Tests whether a file exists on the asset server"""

#        metadata = self.get_metadata(share_name, path)
#        if metadata is not None:
#            return metadata['exists']
#        else:
#            return False

        if path == "":
            return True

        # Note: this is a bit convoluted, but more efficient:
        # We don't directly test the file, we test whether the parent has it as a child
        npath = ntpath.normpath(path)
        parent_path = ntpath.dirname(npath)
        filename = ntpath.basename(npath)

        dirlist = self.get_dir_listing(share_name, parent_path)
        # The SMB protocol requires case-insensitive filenames, whether the client is on Windows or Linux
        if filename.lower() in [s.lower() for s in dirlist]:
            return True
        else:
            return False

        # # Windows needs case-insensitive comparision, Linux needs case sensitive.
        # if settings.CLIENT_TYPE == settings.CLIENT_TYPE_WINDOWS:
        #     if filename.lower() in [s.lower() for s in dirlist]:
        #         return True
        #     else:
        #         return False
        # elif settings.CLIENT_TYPE == settings.CLIENT_TYPE_LINUX:
        #     if filename in dirlist:
        #         return True
        #     else:
        #         return False
        # else:
        #     raise RuntimeError('Unrecognized client operating system: %s' % settings.CLIENT_TYPE)
