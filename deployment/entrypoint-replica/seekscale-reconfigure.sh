#!/bin/sh

. /usr/local/share/seekscale/smbproxy/venv/bin/activate
exec python /usr/local/share/seekscale/seekscale-reconfigure.py "$@"
