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

# Install raw_nginx_cache
mkdir -p /usr/local/share/seekscale/raw_nginx_cache
virtualenv /usr/local/share/seekscale/raw_nginx_cache/venv
source /usr/local/share/seekscale/raw_nginx_cache/venv/bin/activate
pip install -q tornado flask
pip install -q ../../seekscale_commons/
deactivate
cp -f raw_nginx_cache/app.py /usr/local/share/seekscale/raw_nginx_cache/


# Configure dependencies
cp -f raw_nginx_cache/nginx.raw_nginx_cache.conf /etc/nginx/conf.d
cp -f ../common/ssl-conf /etc/nginx
cp -f supervisord.conf /etc/seekscale


# Setup redis
cp -f redis.conf /etc/redis/redis.conf
service redis-server restart

# Setup stunnel
cp -f stunnel.conf /etc/seekscale

# Setup samba
useradd -m cluster_user || true
wget -O /tmp/samba4-installed.tar.xz http://37.187.136.21/bootstrap_files/samba4-installed.2.tar.xz
tar -C /usr/local -xf /tmp/samba4-installed.tar.xz || exit 1
mkdir -p /home/data/smbshares
chmod 777 /home/data/smbshares
mkdir -p /usr/local/samba/alt


# Setup /etc/hosts
echo "127.0.0.1 entrypoint.seekscale.com" >> /etc/hosts


if [ ! -f /etc/smbproxy4.conf ]; then
    cp -f smbproxy4.conf /etc/smbproxy4.conf
fi


# Setup seekscale-reconfigure
rm -f /usr/local/bin/seekscale-reconfigure
ln -sT /usr/local/share/seekscale/seekscale-reconfigure.sh /usr/local/bin/seekscale-reconfigure
rm -f /usr/local/bin/seekscale-check
ln -sT /usr/local/share/seekscale/seekscale-check.sh /usr/local/bin/seekscale-check



# Copy certificates to /etc/seekscale/certs
# Adapt /etc/smbproxy4.conf



# For each remote server $SERVER
# mkdir -p /usr/local/samba/alt/$SERVER
# mkdir -p /usr/local/samba/alt/$SERVER/private
# mkdir -p /usr/local/samba/alt/$SERVER/pid
# mkdir -p /usr/local/samba/alt/$SERVER/lock
# mkdir -p /home/data/smbshares/$SERVER
# chmod 777 /home/data/smbshares/$SERVER
# chown cluster_user:cluster_user /home/data/smbshares/$SERVER
# mkdir -p /home/data/smbshares/$SERVER/.seekscale_tmp

# Create /usr/local/samba/etc/smb-$SERVER.conf
# /bin/echo -e \"cluster_user_password\\ncluster_user_password\\n\" | /usr/local/samba/bin/smbpasswd -c /usr/local/samba/etc/smb-$SERVER.conf -s -a cluster_user

# For each share
# mkdir -p /home/data/smbshares/$SERVER/$UPPERCASE_SHARE
# chmod 777 /home/data/smbshares/$SERVER/$UPPERCASE_SHARE
# chown cluster_user:cluster_user /home/data/smbshares/$SERVER/$UPPERCASE_SHARE



#/etc/hosts
#entrypoint.seekscale.com
#gateway.seekscale.com

# In /etc/smbproxy4.conf, required:
# ssl_cert
# ssl_key
# ssl_ca




#Copier la config samba depuis puppet



#metadata_proxy -> export PYTHONPATH='.'
#raw_nginx_cache

#Créer le dossier /home/data/smbshares/WIN-9LHJ7FU43T7/Z et le chown cluster_user chmod 777


# In metadata_loader.py, set redis port to 6389




#python __main__.py --shares-root /home/data/smbshares/WIN-9LHJ7FU43T7 --metadata-proxy-address 127.0.0.1 --fileserver-address gateway.seekscale.com --fileserver-port 61100
#/usr/local/samba/sbin/smbd -F -s /usr/local/samba/etc/smb-WIN-9LHJ7FU43T7.conf



#/usr/local/share/seekscale/smbproxy/venv/bin/python /usr/local/share/seekscale/smbproxy/metadata_proxy/metadata_proxy.py
#/usr/local/share/seekscale/smbproxy/venv/bin/python /usr/local/share/seekscale/smbproxy/__main__.py --shares-root /home/data/smbshares/WIN-DQ8IVRQPK51 --metadata-proxy-address 127.0.0.1 --fileserver-address gateway.seekscale.com --fileserver-port 61100
#/usr/local/share/seekscale/raw_nginx_cache/venv/bin/python /usr/local/share/seekscale/raw_nginx_cache/app.py
#/usr/local/samba/sbin/smbd -F -s /usr/local/samba/etc/smb-WIN-DQ8IVRQPK51.conf