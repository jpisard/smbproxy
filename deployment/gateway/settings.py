# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import yaml
import platform
import os
from sys import exit

current_os = platform.system()
if current_os == 'Windows':
    install_directory = 'C:\\Luna\\gateway'
    config_directory = 'C:\\Luna\\gateway'
elif current_os == 'Linux':
    install_directory = '/usr/local/share/seekscale_gateway'
    config_directory = '/etc/seekscale'
else:
    raise RuntimeError('Unrecognized platform: %s' % platform.system())

CONFIG_PATH = os.path.join(config_directory, 'gateway.yaml')
TOKEN_FILE_PATH = os.path.join(config_directory, 'secret.key')
SMB_CREDENTIALS_FILE = os.path.join(config_directory, 'smb_creds')


try:
    with open(CONFIG_PATH, 'rb') as config_fh:
        settings = yaml.load(config_fh)
        print("Read config file %s" % CONFIG_PATH)
except Exception:
    settings = dict()


#
# API-related parameters
#
try:
    with open(TOKEN_FILE_PATH, 'r') as token_f:
        AUTH_TOKEN = token_f.read()
except Exception:
    AUTH_TOKEN = None

AUTH_TOKEN = os.getenv('SEEKSCALE_AUTH_TOKEN', AUTH_TOKEN)

API_BASE = 'https://portal.seekscale.com/gateway_api/'

#
# Network parameters
#
public_interface = settings.get('public_network_interface', 'eth0')

#
# SSL parameters
#
ssl_cert = settings.get('ssl_cert', None)
if ssl_cert is None:
    raise RuntimeError('ssl_cert must be set in config file')
elif not os.path.exists(ssl_cert):
    raise RuntimeError('ssl_cert path (%s) is not readable' % ssl_cert)
ssl_key = settings.get('ssl_key', None)
if ssl_key is None:
    raise RuntimeError('ssl_key must be set in config file')
elif not os.path.exists(ssl_key):
    raise RuntimeError('ssl_key path (%s) is not readable' % ssl_key)
ssl_ca = settings.get('ssl_ca', None)
if ssl_ca is None:
    raise RuntimeError('ssl_ca must be set in config file')
elif not os.path.exists(ssl_ca):
    raise RuntimeError('ssl_ca path (%s) is not readable' % ssl_ca)

#
# The remote host (= entrypoint ip)
#
remote_host = settings.get('remote_host', '10.91.0.1')


#
# Environment setup
#
drives_mapping = settings.get('drives_mapping', {})
unc_shares = settings.get('unc_shares', [])
unc_static_mappings = settings.get('unc_static_mappings', {})
ip_mappings = settings.get('ip_mappings', [])

#
# VPN parameters
#
vpn_autoconnect = settings.get('vpn_autoconnect', True)

if platform.system() == 'Windows':
    vpn_config_directory = config_directory
    vpn_process_name = u'openvpn.exe'
elif platform.system() == 'Linux':
    vpn_config_directory = '/etc/openvpn'
    vpn_process_name = 'openvpn'
else:
    raise RuntimeError('Unrecognized platform: %s' % platform.system())
vpn_pid_file = os.path.join(config_directory, 'openvpn.pid')
vpn_booking_id_file = os.path.join(config_directory, 'booking.id')

#
# Metadata service
#
# Time (in s) during which a file_metadata is kept in cache
file_metadata_cache_duration = int(settings.get('file_metadata_cache_duration', 5))
listdir_cache_duration = int(settings.get('listdir_cache_duration', 5))

#
# Common configuration
#

# The storage info for the cache client
storage_info = {
    'type': 'FTP+redis',
    'ftp_host': '10.91.0.1',
    'ftp_root': 'files/',  # Don't forget the trailing '/' !
    'redis_host': '10.91.0.1'
}


#
# Mountpoints
#
SEEKSCALE_MOUNTPOINTS_ROOT = settings.get('mountpoints_root', '/mnt/seekscale_mounts')
