#!/bin/sh

. /home/smbproxy4/venv/bin/activate
exec python /home/smbproxy4/smbproxy.egg "$@"
