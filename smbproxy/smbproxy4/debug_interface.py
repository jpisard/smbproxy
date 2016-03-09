# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import copy
import json
import os
import platform
import traceback

from fs_cache import FSCacheHTTPConnector
import logger


REPR_DICT_SIZE_THRESHOLD = 50


def repr_dict(d):
    ret = dict()

    if isinstance(d, dict):
        ret['size'] = len(d.keys())

        if len(d.keys()) < REPR_DICT_SIZE_THRESHOLD:
            ret['values'] = copy.copy(d)
    elif isinstance(d, list):
        ret['size'] = len(d)

        if len(d) < REPR_DICT_SIZE_THRESHOLD:
            ret['values'] = copy.copy(d)

    return ret


def get_debug_stats_struct(server_factory, listen_address, listen_port):
    fslocalcacheclient = server_factory.fscacheclient
    fscache = fslocalcacheclient.fscache
    metadata_cache = fscache.metadata_cache

    output = dict()

    output['Global'] = dict()

    output['Global']['pid'] = os.getpid()
    output['Global']['listen_address'] = listen_address
    output['Global']['listen_port'] = listen_port

    output['Global']['shutdown_requested'] = server_factory.shutdown_requested

    output['FSLocalCacheClient'] = dict()
    output['FSLocalCacheClient']['active_actions'] = repr_dict(fslocalcacheclient.active_actions)

    output['MetadataCache'] = dict()
    output['MetadataCache']['size'] = len(metadata_cache.keys())

    output['HTTPConnector'] = copy.copy(FSCacheHTTPConnector.requests_stats)

    output['Client'] = []
    for client in server_factory.clients:
        cl_data = {
            'host': client.transport.getPeer().host,
            'tree_connect_requests': repr_dict(client.tree_connect_requests),
            'file_open_requests': repr_dict(client.file_open_requests),
            'file_close_requests': repr_dict(client.file_close_requests),
            'open_files': repr_dict(client.open_files.values()),
            'client_pending_packets_queue_len': client.client_pending_packets_queue_len,
            'server_pending_packets_queue_len': client.server_pending_packets_queue_len,
            'total_processed_client_packets': client.total_processed_client_packets,
            'total_processed_server_packets': client.total_processed_server_packets,
        }
        output['Client'].append(cl_data)

    return output


def dump_debug_stats(server_factory, listen_address, listen_port, stream_stats_client):
    log = logger.logger.new()

    output_file = '/tmp/smbproxy-%d.stats' % os.getpid()

    try:
        stats_data = get_debug_stats_struct(server_factory, listen_address, listen_port)
        stats_data_json = json.dumps(stats_data, indent=4)

        # Write to the output file
        with open(output_file, 'w') as fh:
            fh.write(stats_data_json)
            fh.flush()

        # Send data to the server
        if stream_stats_client is not None:
            stream_stats_client.send_message('%s.smbproxy.%s' % (platform.node(), os.getpid()), stats_data)

    except Exception:
        log.msg(traceback.format_exc(), level=logger.WARN)