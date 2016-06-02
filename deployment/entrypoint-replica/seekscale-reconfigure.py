# coding: utf-8

# Copyright Luna Technology 2016
# Matthieu Riviere <mriviere@luna-technology.com>

import grp
import pwd
import os
import subprocess

import yaml

from seekscale_commons.base import create_dir


def check_config(config):
    required_values = [
        'ssl_cert',
        'ssl_key',
        'ssl_ca',
        'remote_host',
        'shares_host',
        'shares_names'
    ]

    for required_value in required_values:
        if required_value not in config:
            raise RuntimeError('Config file is missing setting for %s' % required_value)


def create_samba_host(host, shares):
    create_dir('/usr/local/samba/alt/%s' % host)
    create_dir('/usr/local/samba/alt/%s/private' % host)
    create_dir('/usr/local/samba/alt/%s/pid' % host)
    create_dir('/usr/local/samba/alt/%s/lock' % host)
    create_dir('/home/data/smbshares/%s' % host)
    os.chmod('/home/data/smbshares/%s' % host, 0o777)

    uid = pwd.getpwnam('cluster_user').pw_uid
    gid = grp.getgrnam('cluster_user').gr_gid
    os.chown('/home/data/smbshares/%s' % host, uid, gid)

    create_dir('/home/data/smbshares/%s/.seekscale_tmp' % host)

    create_samba_config(host, shares)
    subprocess.check_call('/bin/echo -e \"cluster_user_password\\ncluster_user_password\\n\" | /usr/local/samba/bin/smbpasswd -c /usr/local/samba/etc/smb-%s.conf -s -a cluster_user' % host,
                          shell=True)

    for share in shares:
        create_samba_share(host, share)


def create_samba_share(host, share):
    upper_share = share.upper()
    uid = pwd.getpwnam('cluster_user').pw_uid
    gid = grp.getgrnam('cluster_user').gr_gid
    create_dir('/home/data/smbshares/%s/%s' % (host, upper_share))
    os.chmod('/home/data/smbshares/%s/%s' % (host, upper_share), 0o777)
    os.chown('/home/data/smbshares/%s/%s' % (host, upper_share), uid, gid)


def create_samba_config(host, shares):
    samba_conf_tpl = """[global]
    workgroup = WORKGROUP
    interfaces = 127.0.0.1
    bind interfaces only = yes
    smb ports = 1445
    server min protocol = SMB2_02
    server max protocol = SMB3_00
    log file = /usr/local/samba/var/log-__HOST__.%R.%m
    max log size = 1000
    syslog = 0

    guest account = cluster_user
    encrypt passwords = true
    passdb backend = tdbsam
    obey pam restrictions = yes

    unix password sync = yes

    passwd program = /usr/bin/passwd %u
    passwd chat = *Enter\snew\s*\spassword:* %n\n *Retype\snew\s*\spassword:* %n\n *password\supdated\ssuccessfully* .
    pam password change = yes
    map to guest = bad user
    usershare allow guests = yes

    private dir = /usr/local/samba/alt/__HOST__/private
    pid directory = /usr/local/samba/alt/__HOST__/pid
    lock directory = /usr/local/samba/alt/__HOST__/lock
"""

    samba_share_tpl = """[__SHARE__]
    comment = __SHARE__
    read only = no
    writable = yes
    path = /home/data/smbshares/__HOST__/__SHARE_UP__
    public = yes
    guest ok = yes
    browsable = yes
    create mask = 0777
    directory mask = 0777
"""

    samba_conf = samba_conf_tpl.replace('__HOST__', host)
    for share in shares:
        share_up = share.upper()
        share_conf = samba_share_tpl.replace('__HOST__', host)
        share_conf = share_conf.replace('__SHARE__', share)
        share_conf = share_conf.replace('__SHARE_UP__', share_up)

        samba_conf += share_conf

    with open('/usr/local/samba/etc/smb-%s.conf' % host, 'w') as fh:
        fh.write(samba_conf)


def create_supervisor_config(host, metadata_proxy_host='127.0.0.1'):
    supervisord_conf_tpl = """[unix_http_server]
file=/tmp/seekscale-entrypoint.supervisor.sock     ; path to your socket file

[supervisord]
logfile=/var/log/seekscale-entrypoint/supervisord.log ; supervisord log file
logfile_maxbytes=50MB                           ; maximum size of logfile before rotation
logfile_backups=10                              ; number of backed up logfiles
loglevel=error                                  ; info, debug, warn, trace
pidfile=/var/run/seekscale-entrypoint.pid          ; pidfile location
nodaemon=false                                  ; run supervisord as a daemon
minfds=1024                                     ; number of startup file descriptors
minprocs=200                                    ; number of process descriptors
user=root                                       ; default user
childlogdir=/var/log/seekscale-entrypoint/         ; where child log files will live

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix:///tmp/seekscale-entrypoint.supervisor.sock ; use a unix:// URL  for a unix socket


[program:smbproxy]
command=/usr/local/share/seekscale/smbproxy/venv/bin/python /usr/local/share/seekscale/smbproxy/__main__.py --shares-root /home/data/smbshares/__HOST__ --force-host __HOST__ --metadata-proxy-address __METADATA_PROXY_HOST__ --fileserver-address gateway.seekscale.com --fileserver-port 61100
stdout_logfile = /var/log/seekscale-entrypoint/smbproxy.log
redirect_stderr = true
autorestart = true

[program:samba4]
command=/usr/local/samba/sbin/smbd -F -s /usr/local/samba/etc/smb-__HOST__.conf
stdout_logfile = /var/log/seekscale-entrypoint/samba4.log
redirect_stderr = true
autorestart = true

[program:redis-metadata]
command=redis-server /var/lib/redis-metadata/redis-metadata.conf
stdout_logfile = /var/log/seekscale-entrypoint/redis-metadata.log
redirect_stderr = true
autorestart = true
"""

    conf = supervisord_conf_tpl.replace('__HOST__', host)
    conf = conf.replace('__METADATA_PROXY_HOST__', metadata_proxy_host)

    with open('/etc/seekscale/supervisord.conf', 'w') as fh:
        fh.write(conf)


def update_hosts_file(remote_gateway_ip, metadata_proxy_ip):
    try:
        with open('/etc/hosts', 'r') as hosts_fh:
            hosts_contents = hosts_fh.read()
    except:
        raise RuntimeError('Could not open /etc/hosts. Aborting.')

    hosts_lines = hosts_contents.splitlines()

    # Filter hosts_lines to remove lines that refer to gateway.seekscale.com or entrypoint.seekscale.com
    new_hosts_lines = []

    for host_line in hosts_lines:
        # Ignore everything after a '#'
        split1 = host_line.split('#', 1)
        if len(split1) > 1:
            real_host_line = split1[0]
        else:
            real_host_line = host_line

        split2 = real_host_line.split(None)

        if len(split2) == 2 and (split2[1] == 'gateway.seekscale.com' or split2[1] == 'entrypoint.seekscale.com'):
            continue
        else:
            new_hosts_lines.append(host_line)

    # Add the new mappings for gateway.seekscale.com and entrypoint.seekscale.com
    new_hosts_lines.append("%s  gateway.seekscale.com\n" % (remote_gateway_ip,))
    new_hosts_lines.append("%s entrypoint.seekscale.com\n" % (metadata_proxy_ip,))

    new_hosts_contents = '\n'.join(new_hosts_lines)

    with open('/etc/hosts', 'w') as hosts_fh:
        hosts_fh.write(new_hosts_contents)


def restart_redis():
    subprocess.check_call(['/usr/bin/env', 'service', 'redis-server', 'restart'])


def restart_nginx():
    subprocess.check_call(['/usr/bin/env', 'service', 'nginx', 'restart'])


def restart_supervisor():
    sock = '/tmp/seekscale-entrypoint.supervisor.sock'
    supervisord_cmd = ['/usr/bin/env', 'supervisord', '-c', '/etc/seekscale/supervisord.conf']
    supervisorctl_cmd = ['/usr/bin/env', 'supervisorctl', '-c', '/etc/seekscale/supervisord.conf']

    if os.path.exists(sock):
        subprocess.check_call(supervisorctl_cmd + ['reload'])
    else:
        subprocess.check_call(supervisord_cmd)


def main():
    with open('/etc/smbproxy4.conf', 'r') as fh:
        config = yaml.load(fh.read())

    check_config(config)
    metadata_proxy_host = config.get('metadata_proxy_host', '127.0.0.1')

    update_hosts_file(config['remote_host'], metadata_proxy_host)

    create_supervisor_config(config['shares_host'], metadata_proxy_host=metadata_proxy_host)
    create_samba_host(config['shares_host'], config['shares_names'])

    restart_redis()
    restart_nginx()
    restart_supervisor()


if __name__ == '__main__':
    main()
