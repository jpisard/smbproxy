#!/usr/bin/env python
# coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import argparse

from twisted.python import log

from smbproxy4 import settings
from smbproxy4.logger import plainJSONStdOutLogger
from smbproxy4.smbproxy4 import init


def main():
#    log.startLogging(sys.stdout)
    log.addObserver(plainJSONStdOutLogger())

    parser = argparse.ArgumentParser('Smbproxy4')
    parser.add_argument('--listen-address', dest='listen_address', default='0.0.0.0')
    parser.add_argument('--listen-port', dest='listen_port', type=int, default=445)
    parser.add_argument('--fileserver-address', dest='fileserver_address', default='gateway.seekscale.com')
    parser.add_argument('--fileserver-port', dest='fileserver_port', type=int, default=15024)
    parser.add_argument('--metadata-proxy-address', dest='metadata_proxy_address', default='10.91.1.254')
    parser.add_argument('--metadata-proxy-port', dest='metadata_proxy_port', type=int, default=25200)

    parser.add_argument('--shares-root', dest='shares_root', default=None)
    parser.add_argument('--remote-samba-host', dest='remote_samba_host', default=None)
    parser.add_argument('--remote-samba-port', dest='remote_samba_port', default=None)

    parsed_args = parser.parse_args()

    if parsed_args.shares_root is not None:
        settings.SHARES_ROOT = parsed_args.shares_root
    if parsed_args.remote_samba_host is not None:
        settings.REMOTE_SAMBA_HOST = parsed_args.remote_samba_host
    if parsed_args.remote_samba_port is not None:
        settings.REMOTE_SAMBA_PORT = int(parsed_args.remote_samba_port)

    init(
        parsed_args.listen_address,
        parsed_args.listen_port,
        parsed_args.fileserver_address,
        parsed_args.fileserver_port,
        parsed_args.metadata_proxy_address,
        parsed_args.metadata_proxy_port,
        settings,
    )


if __name__ == "__main__":
    main()
