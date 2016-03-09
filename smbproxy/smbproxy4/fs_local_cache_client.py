# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

from datetime import datetime
import ntpath
import os
import pwd
import shutil
import traceback
import uuid

import redis
from twisted.internet import defer

import logger
import audit_logger
from statsd_logging import StatsdClient


class RedisAccessTimeCache(object):
    """
    An interface to the cache that stores the last_access_time for each path
    """
    def __init__(self, redis_host='127.0.0.1'):
        self.redis_host = redis_host
        self.redis = redis.StrictRedis(host=self.redis_host, port=6379, db=0)

    def write_last_access_time(self, share_name, path):
        access_time = datetime.utcnow()
        access_time_string = access_time.isoformat()
        key = 'smbproxy:last_access_time:%s:%s' % (share_name.encode('UTF-8'), path.encode('UTF-8'))
        self.redis.set(key, access_time_string)


class ActionLogger(object):
    """
    An interface to the various loggers/audit systems that watch the actions
    """
    def __init__(self, settings):
        self.audit_logger = audit_logger.AuditLogger(settings)
        self.stats_client = StatsdClient.get()
        self.active_actions = dict()

    def set_current_action(self, log, start_timestamp):
        peer = log._context['peer']
        share_name = log._context['share_name']
        path = log._context['path']
        action_type = log._context['action_type']
        action_id = log._context['action_id']

        self.active_actions[action_id] = {
            'peer': peer,
            'share_name': share_name,
            'path': path,
            'action_type': action_type,
            'action_id': action_id,
            'start_timestamp': start_timestamp.isoformat(),
        }

    def init_action(self, conn_logger, action_type, share_name, path):
        action_id = str(uuid.uuid4())

        log = conn_logger.bind(
            action_id=action_id,
            action_type=action_type,
            share_name=share_name,
            path=path,
        )
        start_timestamp = datetime.utcnow()
#        log.msg("Action %s %s:%s starting" % (action_type, share_name, path), level=logger.INFO)
        self.stats_client.incr('action.%s.started' % action_type)

        self.set_current_action(log, start_timestamp)

        return log, start_timestamp

    @defer.inlineCallbacks
    def finish_action(self, log, start_timestamp, ctxt=None):
        end_timestamp = datetime.utcnow()

        action_id = log._context['action_id']

        action = self.active_actions[action_id]

        ms = (end_timestamp-start_timestamp).total_seconds()*1000
        # log.msg('Operation completed in %sms' % ms, operation_length=ms, level=logger.INFO)
        self.stats_client.incr('action.%s.succeeded' % action['action_type'])
        self.stats_client.timing('action.%s.duration' % action['action_type'], int(ms))

        if (action['action_type'] != 'SYNC' and action['action_type'] != 'LISTDIR')\
                or (action['action_type'] == 'SYNC' and ctxt is not None and ctxt['is_file']):
            try:
                yield self.audit_logger.log(log, start_timestamp, ms, "SUCCESS")
            except Exception:
                # Swallow any error returned by the logging step
                log.msg('Writing to audit_logger failed: %s' % traceback.format_exc(), level=logger.WARN)

        # Remove the action from the list of actions in progress
        del self.active_actions[action_id]

        defer.returnValue(None)

    @defer.inlineCallbacks
    def action_failed(self, log, start_timestamp, ctxt=None):
        end_timestamp = datetime.utcnow()

        action_id = log._context['action_id']

        action = self.active_actions[action_id]

        ms = (end_timestamp-start_timestamp).total_seconds()*1000
        log.msg('Error: Action failed', operation_length=ms, level=logger.ERROR)
        self.stats_client.incr('action.%s.failed' % action['action_type'])
        self.stats_client.timing('action.%s.duration' % action['action_type'], int(ms))

        try:
            yield self.audit_logger.log(log, start_timestamp, ms, "FAILURE")
        except Exception:
            # Swallow any error returned by the logging step
            log.msg('Writing to audit_logger failed: %s' % traceback.format_exc(), level=logger.WARN)

        del self.active_actions[action_id]

        defer.returnValue(None)


class FS(object):
    def __init__(self, settings):
        self.SHARES_ROOT = settings.SHARES_ROOT
        self.required_uid = pwd.getpwnam("cluster_user").pw_uid
        self.stats_client = StatsdClient.get()
        self.MTIME_REFRESH_THRESHOLD = settings.MTIME_REFRESH_THRESHOLD

    @staticmethod
    def listdir(path):
        return os.listdir(path)

    def listdir_n(self, file_metadata):
        path = self.network_path_to_local_path(file_metadata)
        return self.listdir(path)

    @staticmethod
    def exists(path):
        return os.path.exists(path)

    def exists_n(self, file_metadata):
        path = self.network_path_to_local_path(file_metadata)
        return self.exists(path)

    @staticmethod
    def join(*args):
        return os.path.join(*args)

    @staticmethod
    def getsize(path):
        return os.path.getsize(path)

    def getsize_n(self, file_metadata):
        path = self.network_path_to_local_path(file_metadata)
        return self.getsize(path)

    @staticmethod
    def isdir(path):
        return os.path.isdir(path)

    def isdir_n(self, file_metadata):
        path = self.network_path_to_local_path(file_metadata)
        return self.isdir(path)

    @staticmethod
    def isfile(path):
        return os.path.isfile(path)

    def isfile_n(self, file_metadata):
        path = self.network_path_to_local_path(file_metadata)
        return self.isfile(path)

    def network_path_to_local_path(self, file_metadata):
        """
        Translate a network path (UNC form) into a path on the filesystem.
        This needs full metadata to perform path normalization.
        :param file_metadata:
        :return:
        """
        (_1, _2, unc_host, unc_share) = file_metadata.share_name.split('\\')

        if file_metadata.has_normalized_path():
            normalized_path = file_metadata.normalized_path()

            # Rewrite the path into a linux one
            unix_path = normalized_path.replace('\\', '/')

            # Remove leading '/' if necessary
            if len(unix_path) > 0 and unix_path[0] == '/':
                unix_path = unix_path[1:]

            return self.join(self.SHARES_ROOT, unc_share.upper(), unix_path).encode('UTF-8')

        else:
            # The file doesn't exist on the distant filesystem. But we need to do local normalization
            path = file_metadata.path

            # Rewrite the path into a linux one
            unix_path = path.replace('\\', '/')

            # Remove leading '/' if necessary
            if len(unix_path) > 0 and unix_path[0] == '/':
                unix_path = unix_path[1:]

            curpath = self.join(self.SHARES_ROOT, unc_share.upper())
            keep_searching = True
            for pathpart in unix_path.split('/'):
                if keep_searching:
                    candidates = self.listdir(curpath)
                    l_pathpart = pathpart.lower()
                    found = False
                    for candidate in candidates:
                        if candidate.lower() == l_pathpart:
                            curpath = self.join(curpath, candidate)
                            found = True
                            break

                    if not found:
                        keep_searching = False
                else:
                    curpath = self.join(curpath, pathpart)

            return curpath.encode('UTF-8')

    def create_directory(self, file_metadata, log):
        """
        Create a directory
        :param file_metadata:
        :param log:
        :return:
        """
        if file_metadata.path == "":
            return
        else:
            local_path = self.network_path_to_local_path(file_metadata)
            if not os.path.exists(local_path):
                try:
                    # FIXME: doesn't work, the directory would be created by another makedirs anyway.
                    # # Sanity check: Ensure we don't create a directory where the same one, but with a different case,
                    # # already exists
                    # # TODO: make sure this doesn't take too much time
                    # parent_dir = os.path.dirname(local_path)
                    # existing_siblings_directories = os.listdir(parent_dir)
                    # for existing_sibling in existing_siblings_directories:
                    #     if existing_sibling.lower() == os.path.basename(local_path).lower():
                    #         log.msg("\tWarning: not creating %s because %s already exists" % (
                    #             local_path, os.path.join(parent_dir, existing_sibling)
                    #         ), level=logger.INFO)
                    #         return

                    log.msg(
                        "Notice: creating %s:%s (local path: %s)" % (
                            file_metadata.share_name, file_metadata.path, local_path
                        ),
                        level=logger.INFO
                    )
                    os.makedirs(local_path, mode=0777)
                    os.chown(local_path, self.required_uid, -1)
                    os.chmod(local_path, 0777)
                except Exception:
                    if os.path.exists(local_path) and os.path.isdir(local_path):
                        # Directory exists, nothing wrong
                        pass
                    else:
                        log.msg("\tWarning: createDirectory(%s, %s) failed." % (
                            file_metadata.share_name, file_metadata.path), level=logger.INFO)
                        self.stats_client.incr('action.SYNC.errors.create_directory_failure')

    def fake_file(self, file_metadata, log):
        """
        Create a fake (empty) file on the local filesystem
        :param file_metadata:
        :param log:
        :return:
        """
        if False:
            return
        else:
            try:
                local_path = self.network_path_to_local_path(file_metadata)
                distant_mtime = file_metadata.mtime()
                distant_size = file_metadata.size()
                if os.path.exists(local_path):
                    # encoded_path = f
                    # error_msg = 'File "%s" already present on %s. No need to fake it.' % (encoded_path, share_name)
                    # log.msg(error_msg, level=logger.INFO)
                    return
                else:
                    with open(local_path, 'wb') as target_fd:
                        if distant_size > 0:
                            target_fd.seek(distant_size-1)
                            target_fd.write('\0')
#                    os.chown(local_path, self.required_uid, -1)
                    os.chmod(local_path, 0600)
                    # We set a slightly old mtime, so that the smbproxy doesn't
                    # think later on that the file is up to date
                    fake_mtime = distant_mtime - 500 * self.MTIME_REFRESH_THRESHOLD
                    os.utime(local_path, (fake_mtime, fake_mtime))
            except Exception:
                log.msg('Error: Could not fake file \"%s\" on %s: %s' % (
                    '\\' + file_metadata.path, file_metadata.share_name, traceback.format_exc()
                ), level=logger.WARN)
                self.stats_client.incr('action.SYNC.errors.could_not_fake_file')

class FSLocalCacheClient(object):
    """The FSLocalCacheClient caches data on a local samba server. Is it shared between all the clients."""

    def __init__(self, fscache, settings, redis_host='127.0.0.1'):
        self.fscache = fscache
        self.settings = settings
        self.SHARES_ROOT = self.settings.SHARES_ROOT
        self.required_uid = pwd.getpwnam("cluster_user").pw_uid
        self.log = None

        self.fs = FS(settings)
        self.redis_at_cache = RedisAccessTimeCache(redis_host=redis_host)

        self.stats_client = StatsdClient.get()
        self.action_logger = ActionLogger(settings)

    @property
    def active_actions(self):
        return self.action_logger.active_actions

    @defer.inlineCallbacks
    def action(self, share_name, path, conn_logger, action_type, perform):
        log, start_timestamp = self.action_logger.init_action(conn_logger, action_type, share_name, path)

        try:
            yield perform(share_name, path, conn_logger, log)
        except Exception:
            log.msg('Action %s failed: %s' % (action_type, traceback.format_exc()), level=logger.ERROR)
            self.action_logger.action_failed(log, start_timestamp)
        else:
            self.action_logger.finish_action(log, start_timestamp)

    @defer.inlineCallbacks
    def perform_sync(self, share_name, path, conn_logger, log):
        self.redis_at_cache.write_last_access_time(share_name, path)

        ctxt = {
            'is_file': False,
            'needs_import': False,
        }

        file_metadata = yield self.fscache.metadata_object(share_name, path, log, include_children=False)

        if not file_metadata.exists():
            # We need to sync the parent directory, just in case we're trying to create a new file in a
            # not-yet-synced directory
            parent_dir = ntpath.dirname(path)
            if parent_dir != path:
                yield self.sync(share_name, parent_dir, conn_logger)
        else:
            # Is it a regular file ? Then download
            if file_metadata.is_file():
                ctxt['is_file'] = True
                # Create the containing directory
                self.create_dir_hierarchy(file_metadata.parent_metadata(), log)
                yield self.send_file(file_metadata, log, ctxt)

            # Is it a directory, then just create it
            elif file_metadata.is_dir():
                # Create the containing directory
                self.create_dir_hierarchy(file_metadata.parent_metadata(), log)
                self.fs.create_directory(file_metadata, log)

    def sync(self, share_name, path, conn_logger):
        """
        Sync a file
        :param share_name:
        :param path:
        :param conn_logger:
        :return: A deferred that fires once the action has completed
        """
        return self.action(share_name, path, conn_logger, 'SYNC', self.perform_sync)

    @defer.inlineCallbacks
    def perform_listdir(self, share_name, path, conn_logger, log):
        # First of all, ensure the directory is properly synced
        yield self.sync(share_name, path, log)

        file_metadata = yield self.fscache.metadata_object(share_name, path, log, include_children=True)
        if file_metadata.exists() and file_metadata.is_dir():
            # Fake the contents
            children_metadata = file_metadata.children_metadata()
            for child_metadata in children_metadata:
                if child_metadata.exists():
                    # If file, fake it
                    if child_metadata.is_file():
                        self.fs.fake_file(child_metadata, log)

                    # If directory, create it
                    elif child_metadata.is_dir():
                        self.fs.create_directory(child_metadata, log)
        else:
            if not file_metadata.exists() and self.fs.exists_n(file_metadata) and self.fs.isdir_n(file_metadata):
                # This means the directory exists in the cache, but not on the source filesystem.
                # Nothing to do in this situation
                pass
            else:
                if not file_metadata.exists():
                    log.msg(
                        'Action LISTDIR on something that does not exist: %s:%s' % (
                            share_name, path
                        ),
                        level=logger.WARN)
                elif not file_metadata.is_dir():
                    log.msg(
                        'Action LISTDIR on something that is not a directory: %s:%s' % (
                            share_name, path
                        ),
                        level=logger.WARN)
                else:
                    log.msg('Unknown error during listdir()', level=logger.ERROR)

    def listdir(self, share_name, path, conn_logger):
        """
        Lists a directory
        :param share_name:
        :param path:
        :param conn_logger:
        :return: A deferred that fires once the action has completed
        """
        return self.action(share_name, path, conn_logger, 'LISTDIR', self.perform_listdir)

    @defer.inlineCallbacks
    def perform_syncback(self, share_name, path, conn_logger, log):
        file_metadata = yield self.fscache.metadata_object(share_name, path, log)
        local_path = self.fs.network_path_to_local_path(file_metadata)
        if self.fs.isfile(local_path):
            yield self.fscache.set_file(file_metadata, local_path, log)
        else:
            log.msg('Error: Requested to sync_back non-existing file %s' % local_path, level=logger.ERROR)
            self.stats_client.incr('action.SYNCBACK.errors.non_existing_file')
            err = RuntimeError('Requested to sync_back non-existing file %s' % local_path)
            raise err

    def sync_back(self, share_name, path, conn_logger):
        """
        Syncs a file back from the local SMB share to the source server
        :param share_name:
        :param path:
        :param conn_logger:
        :return: A deferred that fires once the action has completed
        """
        return self.action(share_name, path, conn_logger, 'SYNCBACK', self.perform_syncback)

    @defer.inlineCallbacks
    def perform_delete(self, share_name, path, conn_logger, log):
        yield defer.maybeDeferred(self.fscache.delete_file, share_name, path, log)

    def delete(self, share_name, path, conn_logger):
        """
        Deletes a file on the master server
        :param share_name:
        :param path:
        :param conn_logger:
        :return: A deferred that fires once the action has completed
        """
        return self.action(share_name, path, conn_logger, 'DELETE', self.perform_delete)

    @defer.inlineCallbacks
    def perform_touch(self, share_name, path, conn_logger, log):
        yield defer.maybeDeferred(self.fscache.touch_file, share_name, path, log)

    def touch(self, share_name, path, conn_logger):
        """
        Creates a blank file on the master server. Used at the beginning of a write.
        :param share_name:
        :param path:
        :param conn_logger:
        :return: A deferred that fires once the action has completed
        """
        return self.action(share_name, path, conn_logger, 'TOUCH', self.perform_touch)

    #
    # Internal data
    #
    def create_dir_hierarchy(self, file_metadata, log):
        """
        Create a directory hierarchy
        :param file_metadata:
        :param log:
        :return:
        """
        if file_metadata is None:
            return

        # The root directory doesn't have to be created
        if file_metadata.path == "":
            return

        # Create parents directories
        parent_metadata = file_metadata.parent_metadata()

        if parent_metadata is not None:
            path = ntpath.normpath(file_metadata.path)
            parent_path = ntpath.normpath(parent_metadata.path)
            if path != parent_path:
                self.create_dir_hierarchy(parent_metadata, log)

        # Create the directory itself
        self.fs.create_directory(file_metadata, log)


    @defer.inlineCallbacks
    def send_file(self, file_metadata, log, ctxt):
        """
        Pull a file from the cache and write in on the local filesystem
        :param file_metadata:
        :param log:
        :param ctxt:
        :return:
        """
        # If the file already exists, we need to check whether it has to be updated or if we keep the local one.
        local_path = self.fs.network_path_to_local_path(file_metadata)
        distant_mtime = file_metadata.mtime()

        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            local_mtime = os.path.getmtime(local_path)
            # FIXME: We should put a >0 threshold to this. mtime can have low resolution. Which would cause the
            # same file to be reloaded endlessly.
            if local_size > 0 and distant_mtime < local_mtime + self.settings.MTIME_REFRESH_THRESHOLD:
                self.stats_client.incr('action.SYNC.info.sync_cache_hit')
                # log.msg('\tNot overwriting because local version is more recent: %s' % local_path, level=logger.INFO)
                defer.returnValue(None)
            else:
                self.stats_client.incr('action.SYNC.info.sync_file_import')
                # log.msg('\tOverwriting: %s' % local_path, level=logger.INFO)

        ctxt['needs_import'] = True

        # Get a fd to the file
        # FIXME: get_file now throws an exception instead of returning None
        try:
            p = yield self.fscache.get_file(file_metadata, log)
        except Exception:
            log.msg('Error: No file fetched for \"%s\" on %s: %s' % (
                '\\' + file_metadata.path, file_metadata.share_name, traceback.format_exc()), level=logger.ERROR)
            self.stats_client.incr('action.SYNC.errors.no_file_fetched')

            try:
                os.chown(local_path, os.getuid(), -1)
                os.chmod(local_path, 600)
                fake_mtime = distant_mtime - 500 * self.settings.MTIME_REFRESH_THRESHOLD
                os.utime(local_path, (fake_mtime, fake_mtime))
            except Exception:
                log.msg(
                    "Error: couldn't mark file \"%s\" on %s (local path: %s) as unavailable: %s" % (
                        '\\' + file_metadata.path, file_metadata.share_name, local_path, traceback.format_exc()),
                    level=logger.WARN
                )

        else:
            try:
                os.rename(p, local_path)
                os.chown(local_path, self.required_uid, -1)
                os.chmod(local_path, 0777)
                os.utime(local_path, (distant_mtime, distant_mtime))
            except Exception:
                log.msg(
                    "Error: couldn't store file \"%s\" on %s (local path: %s): %s" % (
                        '\\' + file_metadata.path, file_metadata.share_name, local_path, traceback.format_exc()),
                    level=logger.WARN
                )
                self.stats_client.incr('action.SYNC.errors.could_not_store_file')


    @staticmethod
    def clean_supplementary_files(share_name, dirname, children_to_keep):
        return

    def flush_metadata_cache(self):
        pass

    def reset(self):
        self.flush_metadata_cache()

        # Delete all files in all the shares
        for sharedir in os.listdir(self.SHARES_ROOT):
            full_share_path = os.path.join(self.SHARES_ROOT, sharedir)
            for f in os.listdir(full_share_path):
                full_f_path = os.path.join(full_share_path, f)
                if os.path.isfile(f):
                    try:
                        os.unlink(full_f_path)
                    except Exception:
                        pass
                elif os.path.isdir(f):
                    try:
                        shutil.rmtree(full_f_path)
                    except Exception:
                        pass
