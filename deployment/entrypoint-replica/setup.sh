#!/bin/bash

set -ex

# Check for pre-existing installation
if [ -d /usr/local/share/seekscale ]; then
    echo "Found existing installation. Removing it"
    rm -rf /usr/local/share/seekscale
fi


# Install dependencies
apt-get update -y -qq
apt-get install -y -qq wget stunnel4 libffi-dev libssl-dev cifs-utils nginx redis-server supervisor libpq-dev python-dev python-virtualenv

# Create directorie hierarchy
mkdir -p /etc/seekscale
mkdir -p /etc/seekscale/certs
mkdir -p /var/log/seekscale-entrypoint
mkdir -p /usr/local/share/seekscale

# Install files
cp -f *.py /usr/local/share/seekscale/
cp -f *.sh /usr/local/share/seekscale/
chmod +x /usr/local/share/seekscale/seekscale-reconfigure.sh
chmod +x /usr/local/share/seekscale/seekscale-check.sh

# Install smbproxy
mkdir -p /usr/local/share/seekscale/smbproxy
virtualenv /usr/local/share/seekscale/smbproxy/venv
source /usr/local/share/seekscale/smbproxy/venv/bin/activate
pip install -q twisted requests pyasn1 redis treq structlog pyyaml psycopg2 statsd tornado
pip install -q ../../seekscale_commons/
deactivate
cp -r ../../smbproxy/* /usr/local/share/seekscale/smbproxy


# Configure dependencies
cp -f ../common/ssl-conf /etc/nginx
cp -f supervisord.conf /etc/seekscale


# Setup redis
mkdir -p /var/lib/redis-metadata
cp -f redis-metadata.conf /var/lib/redis-metadata/redis-metadata.conf
chown -R redis:redis /var/lib/redis-metadata


# Setup samba
useradd -m cluster_user || true
wget -O /tmp/samba4-installed.tar.xz http://37.187.136.21/bootstrap_files/samba4-installed.2.tar.xz
tar -C /usr/local -xf /tmp/samba4-installed.tar.xz || exit 1
mkdir -p /home/data/smbshares
chmod 777 /home/data/smbshares
mkdir -p /usr/local/samba/alt


if [ ! -f /etc/smbproxy4.conf ]; then
    cp -f smbproxy4.conf /etc/smbproxy4.conf
fi


# Setup seekscale-reconfigure
rm -f /usr/local/bin/seekscale-reconfigure
ln -sT /usr/local/share/seekscale/seekscale-reconfigure.sh /usr/local/bin/seekscale-reconfigure
rm -f /usr/local/bin/seekscale-check
ln -sT /usr/local/share/seekscale/seekscale-check.sh /usr/local/bin/seekscale-check

