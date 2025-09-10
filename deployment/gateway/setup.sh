#!/bin/bash

set -ex

# Check for pre-existing installation
if [ -d /usr/local/share/seekscale ]; then
    echo "Found existing installation. Removing it"
    rm -rf /usr/local/share/seekscale
fi


# Install dependencies
apt-get update -y -qq
apt-get install -y -qq wget build-essential  stunnel4 libffi-dev libssl-dev cifs-utils nginx supervisor libpq-dev python2.7-dev
if [  ! -f get-pip.py ]; then
	wget https://bootstrap.pypa.io/pip/2.7/get-pip.py
fi
python2.7 get-pip.py
python2.7 -m pip install virtualenv

# Create directory hierarchy
mkdir -p /etc/seekscale
mkdir -p /etc/seekscale/certs
mkdir -p /var/log/seekscale-gateway
mkdir -p /usr/local/share/seekscale
mkdir -p /mnt/seekscale_mounts

# Install files
cp -f *.py /usr/local/share/seekscale/
cp -f *.sh /usr/local/share/seekscale/
chmod +x /usr/local/share/seekscale/seekscale-reconfigure.sh
chmod +x /usr/local/share/seekscale/seekscale-check.sh

# Create virtualenv
virtualenv /usr/local/share/seekscale/venv
source /usr/local/share/seekscale/venv/bin/activate
pip install -q  statsd==3.3 Twisted==14.0.2 tornado==4.1 pyyaml redis psutil requests futures poster jinja2 ujson pyOpenSSL ndg-httpsclient pyasn1
pip install -q ../../seekscale_commons/

# Configure dependencies
cp -f seekscale-gateway.nginx.conf /etc/nginx/conf.d
cp -f ../common/ssl-conf /etc/nginx
cp -f supervisord.conf /etc/seekscale

# Setup stunnel
cp -f stunnel.conf /etc/seekscale

# Setup /etc/hosts
echo "127.0.0.1 gateway.seekscale.com" >> /etc/hosts


if [ ! -f /etc/seekscale/gateway.yaml ]; then
    cp -f gateway.yaml /etc/seekscale/gateway.yaml
fi


# Setup seekscale-reconfigure
rm -f /usr/local/bin/seekscale-reconfigure
ln -sT /usr/local/share/seekscale/seekscale-reconfigure.sh /usr/local/bin/seekscale-reconfigure
rm -f /usr/local/bin/seekscale-check
ln -sT /usr/local/share/seekscale/seekscale-check.sh /usr/local/bin/seekscale-check


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
