# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import yaml

CONFIG_PATH = '/etc/smbproxy4.conf'

try:
    with open(CONFIG_PATH, 'rb') as config_fh:
        settings = yaml.load(config_fh)
except Exception:
    print "Warning! No config file found! Using defaults."
    settings = dict()

# The size above which we download a file through CacheClient3
CACHECLIENT3_SIZE_THRESHOLD = settings.get('cacheclient3_size_threshold', 1*1024*1024)

# The minimal time delay, in seconds, between file changes that we acknowledge.
# Note that this is *if* the metadata cache has been flushed. By default, no file changes are acknowledged.
#
# In other words, for a change to be propagated from the VM to the slave:
# - the metadata cache must be flushed
# - the file on the VM must have a mtime that is at least MTIME_REFRESH_THRESHOLD seconds after the mtime of the file
# on the slave node
#
# This is necessary to handle the fact that we can't set mtimes with arbitrary resolution (ie we can't set slave_mtime
# = vm_mtime) when we propagate a file to the slave
MTIME_REFRESH_THRESHOLD = settings.get('mtime_refresh_threshold', 5)


# Minimal time between two checks of the metadata
MTIME_METADATA_REFRESH_THRESHOLD = settings.get('mtime_metadata_refresh_threshold', 15)
NO_RECHECK_METADATA_PATTERNS = settings.get('no_recheck_metadata_patterns', [])

# Whether files get written back to the control server
ENABLE_WRITE_THROUGH = settings.get('enable_write_through', True)


# Whether the proxy issues a touch() command when a file is opened in write mode.
# This gives the illusion, on the studio side, that the file is currently being written.
# On the other hand:
#   * this is only an illusion. In particular, the file isn't locked as it would be on a real filesystem
#   * it seems to cause issues, because the studio can believe that the file has been written blank and bail at that
#
# The value to give to this is quite workflow-dependent.
# For maya setups where all rendering happens locally, it's probably simpler to disable it.
# Otoh, when the render happens directly on a network share and multiple jobs collaborate using lockfiles,
# this is necessary.
ENABLE_TOUCH_FILES = settings.get('enable_touch', False)

# Whether to enable verbose output
DEBUG_OUTPUT = settings.get('debug_output', False)

# Whether to enable logging of each SMB2 packet (except READ/WRITE packets)
LOG_SMB2_PACKETS = settings.get('log_smb2_packets', False)

# Number of packets pending in queue above which a warning gets printed
PENDING_PACKETS_LEVEL_WARN = 100


# Maximum time (in seconds) allowed to be spent in a list_dir request
LIST_DIR_TIMEOUT = settings.get('list_dir_timeout', 50)


REDIS_FILE_TRANSFERBACK_HOST = settings.get('redis_file_transferback_host', '127.0.0.1')


ENABLE_AUDIT_LOG = settings.get('enable_audit_log', True)
AUDIT_LOG_HOST = settings.get('audit_log_host', '127.0.0.1')

ENABLE_CENTRAL_STATS_FORWARD = settings.get('enable_central_stats_forward', True)
CENTRAL_STATS_SERVER_HOST = settings.get('central_stats_server_host', '127.0.0.1')

STATSD_HOST = settings.get('statsd_host', '127.0.0.1')
STATSD_PORT = int(settings.get('statsd_port', 8125))


SHARES_ROOT = settings.get('shares_root', '/home/data/smbshares/')
REMOTE_SAMBA_HOST = settings.get('remote_samba_host', '127.0.0.1')
REMOTE_SAMBA_PORT = int(settings.get('remote_samba_port', 1445))

FORCE_HOST = settings.get('force_host', None)

cache_host = settings.get('cache_host', '127.0.0.1')
ssl_cert = settings.get('ssl_cert', None)
ssl_key = settings.get('ssl_key', None)
ssl_ca = settings.get('ssl_ca', None)

gateway_host = settings.get('gateway_host', 'gateway.seekscale.com')
gateway_port = int(settings.get('gateway_port', 61100))
