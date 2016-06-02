#!/bin/sh

exec supervisorctl -c /etc/seekscale/supervisord.conf status
