#!/bin/bash

set -ex

# Install dependencies
apt-get update -y -qq
apt-get install -y -qq libffi-dev libssl-dev cifs-utils nginx redis-server supervisor libpq-dev python-dev python-virtualenv

# Create directory hierarchy
mkdir -p /etc/seekscale
mkdir -p /etc/seekscale/certs
mkdir -p /var/log/seekscale-gateway
mkdir -p /usr/local/share/seekscale
mkdir -p /mnt/seekscale_mounts

# Install files
cp -f *.py /usr/local/share/seekscale/

# Create virtualenv
virtualenv /usr/local/share/seekscale/venv
source /usr/local/share/seekscale/venv/bin/activate
pip install tornado twisted pyyaml redis psutil requests futures poster jinja2 ujson pyOpenSSL ndg-httpsclient pyasn1
pip install ../../seekscale_commons/

# Configure dependencies
cp -f seekscale-gateway.nginx.conf /etc/nginx/conf.d
cp -f ../common/ssl-conf /etc/nginx
cp -f supervisord.conf /etc/seekscale

# Setup /etc/hosts
echo "127.0.0.1 gateway.seekscale.com" >> /etc/hosts



# Remaining tasks:
# Copy certificates in /etc/seekscale/certs
# Create credentials files /etc/seekscale/smb_creds
# Create config file /etc/seekscale/gateway.yaml
# Mount drives
# Add entrypoint.seekscale.com in /etc/hosts
# Start supervisor (supervisord -c /etc/seekscale/supervisord.conf)


# Required in gateway.yaml:
# ssl_cert
# ssl_key
# ssl_ca
# remote_host


# Typical format for smb_creds:
#username=Administrator
#password=v9Cq$iSFJ?



#from mount_drives import MountPoint
#m2 = MountPoint("\\\\WIN-9LHJ7FU43T7\\z")
#m2.is_mounted()
#m2.mount()