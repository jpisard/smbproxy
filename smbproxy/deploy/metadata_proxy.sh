#!/bin/sh

. /home/smbproxy4/venv/bin/activate
exec python /home/smbproxy4/venv/lib/python2.7/site-packages/metadata_proxy/metadata_proxy.py "$@"
