# coding: utf-8

# Copyright Luna Technology 2015
# Matthieu Riviere <mriviere@luna-technology.com>

import base64
import json
import logging
import ntpath
import time
import traceback
import zlib

import redis


METADATA_VALIDITY_DURATION = 60

logger = logging.getLogger(__name__)

redis_conn = None


def get_redis_conn():
    global redis_conn
    if redis_conn is None:
        #redis_conn = redis.StrictRedis(port=6380)
        redis_conn = redis.StrictRedis(port=6379)

    return redis_conn


def compute_file_metadata_key(path):
    encoded_path = base64.b64encode(path.encode('UTF-8'))
    return "seekscale:metadata:file_metadata:%s" % encoded_path


def compute_list_dir_key(path):
    encoded_path = base64.b64encode(path.encode('UTF-8'))
    return "seekscale:metadata:list_dir:%s" % encoded_path


def get_cached_list_dir(directory, max_age=METADATA_VALIDITY_DURATION):
    """
    Queries the cache for a list_dir response
    :param directory: the directory we want to list
    :param max_age:
    :return: the cached data if it is valid (using the same format as the list_dir response) or None
    """
    redis_conn = get_redis_conn()
    key = compute_list_dir_key(directory)
    v_raw = redis_conn.get(key)
    if v_raw is not None:
        v = json.loads(zlib.decompress(v_raw))
        if time.time() - v['_update_time'] < max_age:
            # Retrieve all the individual file_metadata from the cache
            # We don't need to check their validity, because, in the worst case, they were updated by the last
            # list_dir.
            # We still check for None, for good measure
            pipe = redis_conn.pipeline()
            for child in v['files']:
                child_full_path = ntpath.join(v['directory'], child)
                child_key = compute_file_metadata_key(child_full_path)
                pipe.get(child_key)

            result = pipe.execute()
            v['files_metadata'] = dict()
            for i in xrange(len(v['files'])):
                child = v['files'][i]
                res = json.loads(zlib.decompress(result[i]))

                if res is None:
                    logger.error('Error: got None for metadata of %s, while listing directory %s.' % (
                        (child, directory)
                    ))
                    # TODO: What do we do then? Return None?

                v['files_metadata'][child] = res

            # Recompute total_size
            v['total_size'] = 0
            for p in v['files_metadata']:
                try:
                    if v['files_metadata'][p]['metadata']['isfile']:
                        v['total_size'] += v['files_metadata'][p]['metadata']['st_size']
                except KeyError:
                    logger.info(
                            u"File \"%s\" in returned by listdir, but doesn't actually exist: %s" % (
                                p, traceback.format_exc()
                            )
                    )

            return v

    return None


def set_cached_list_dir(data):
    """
    Updates the cache for a /list_dir response.
    :param data: the response data from a /list_dir.json call
    :return: None
    """
    directory = data['directory']
    update_time = time.time()
    data['_update_time'] = update_time

    redis_conn = get_redis_conn()
    pipe = redis_conn.pipeline()
    dir_key = compute_list_dir_key(directory)

    stored_dir_data = {
        'directory': data['directory'],
        'files': data['files'],
        '_update_time': update_time,
    }

    v_raw = zlib.compress(json.dumps(stored_dir_data))
    pipe.set(dir_key, v_raw)

    for child in data['files']:
        file_metadata_data = data['files_metadata'][child]
        file_metadata_data['_update_time'] = update_time
        full_child_path = ntpath.join(directory, child)
        key = compute_file_metadata_key(full_child_path)
        v_raw = zlib.compress(json.dumps(file_metadata_data))
        pipe.set(key, v_raw)

    pipe.execute()


def get_cached_file_metadata(path, max_age=METADATA_VALIDITY_DURATION):
    redis_conn = get_redis_conn()
    key = compute_file_metadata_key(path)
    v_raw = redis_conn.get(key)
    if v_raw is not None:
        v = json.loads(zlib.decompress(v_raw))
        if time.time() - v['_update_time'] < max_age:
            return v

    return None


def set_cached_file_metadata(data):
    path = data['path']

    data['_update_time'] = time.time()

    redis_conn = get_redis_conn()
    key = compute_file_metadata_key(path)

    v_raw = zlib.compress(json.dumps(data))

    redis_conn.set(key, v_raw)


def flush_metadata_cache():
    redis_conn = get_redis_conn()
    redis_conn.flushall()
