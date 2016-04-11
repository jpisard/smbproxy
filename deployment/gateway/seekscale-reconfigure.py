# coding: utf-8

# Copyright Luna Technology 2016
# Matthieu Riviere <mriviere@luna-technology.com>

import os.path
import subprocess

import yaml

from mount_drives import MountPoint


def check_config(config):
    required_values = [
        'ssl_cert',
        'ssl_key',
        'ssl_ca',
        'remote_host',
        'smb_username',
        'smb_password',
        'shares'
    ]

    for required_value in required_values:
        if required_value not in config:
            raise RuntimeError('Config file is missing setting for %s' % required_value)


def smb_create_credentials_file(username, password):
    with open('/etc/seekscale/smb_creds', 'w') as fh:
        fh.write("""username=%s
password=%s
""" % (username, password))


def mount_drive(full_share):
    m2 = MountPoint(full_share)
    m2.mount()

    if not m2.is_mounted():
        raise RuntimeError('Could not mount drive %s' % full_share)


def update_hosts_file(remote_entrypoint_ip):
    try:
        with open('/etc/hosts', 'r') as hosts_fh:
            hosts_contents = hosts_fh.read()
    except:
        raise RuntimeError('Could not open /etc/hosts. Aborting.')

    hosts_lines = hosts_contents.splitlines()

    # Filter hosts_lines to remove lines that refer to entrypoint.seekscale.com
    new_hosts_lines = []

    for host_line in hosts_lines:
        # Ignore everything after a '#'
        split1 = host_line.split('#', 1)
        if len(split1) > 1:
            real_host_line = split1[0]
        else:
            real_host_line = host_line

        split2 = real_host_line.split(None)

        if len(split2) == 2 and split2[1] == 'entrypoint.seekscale.com':
            continue
        else:
            new_hosts_lines.append(host_line)

    # Add the new mapping for entrypoint.seekscale.com
    new_hosts_lines.append("%s  entrypoint.seekscale.com\n" % (remote_entrypoint_ip,))

    new_hosts_contents = '\n'.join(new_hosts_lines)

    with open('/etc/hosts', 'w') as hosts_fh:
        hosts_fh.write(new_hosts_contents)


def disable_local_redis():
    subprocess.check_call(['/usr/bin/env', 'service', 'redis-server', 'stop'])


def restart_nginx():
    subprocess.check_call(['/usr/bin/env', 'service', 'nginx', 'restart'])


def restart_supervisor():
    sock = '/tmp/seekscale-gateway.supervisor.sock'
    supervisord_cmd = ['/usr/bin/env', 'supervisord', '-c', '/etc/seekscale/supervisord.conf']
    supervisorctl_cmd = ['/usr/bin/env', 'supervisorctl', '-c', '/etc/seekscale/supervisord.conf']

    if os.path.exists(sock):
        subprocess.check_call(supervisorctl_cmd + ['reload'])
    else:
        subprocess.check_call(supervisord_cmd)


def main():
    with open('/etc/seekscale/gateway.yaml', 'r') as fh:
        config = yaml.load(fh.read())

    check_config(config)

    smb_create_credentials_file(config['smb_username'], config['smb_password'])

    drives = config['shares']
    for drive in drives:
        mount_drive(drive)

    update_hosts_file(config['remote_host'])

    disable_local_redis()
    restart_nginx()
    restart_supervisor()


if __name__ == '__main__':
    main()
