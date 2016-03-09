#!/usr/bin/env python
#  coding: utf-8

# Copyright Luna Technology 2014
# Matthieu Riviere <mriviere@luna-technology.com>

import argparse
import datetime
import sys
import traceback

from smb.SMBConnection import SMBConnection


# Example values:
# server-ip: 127.0.0.1
# server-port: 30000
# server-name: 10.0.60.20
# share-name: Renders


def main():
    parser = argparse.ArgumentParser('check_smbproxy')
    parser.add_argument('server_ip', metavar='server-ip')
    parser.add_argument('server_port', metavar='server-port', type=int)
    parser.add_argument('server_name', metavar='server-name')
    parser.add_argument('share_name', metavar='share-name')
    parser.add_argument('--path', default='/')
    parser.add_argument('--user', default='cluster_user')
    parser.add_argument('--password', default='cluster_user_password')


    parsed_args = parser.parse_args()

    userID = parsed_args.user
    password = parsed_args.password
    client_machine_name = 'test_client'

    server_ip = parsed_args.server_ip
    server_port = parsed_args.server_port
    server_name = parsed_args.server_name
    share_name = parsed_args.share_name
    path = parsed_args.path

    try:
        start_time = datetime.datetime.utcnow()
        conn = SMBConnection(userID, password, client_machine_name, server_name, use_ntlm_v2=False, is_direct_tcp=True)
        assert conn.connect(server_ip, server_port)

        ls = conn.listPath(share_name, path)
        num_files = len(ls)
#        for f in ls:
#            print f.filename

        conn.close()
        end_time = datetime.datetime.utcnow()
        time_spent = (end_time-start_time).total_seconds()*1000

        print "OK: %d files found in %s | connection_time=%dms" % (num_files, path, time_spent)
    except Exception:
        print "CRITICAL: Exception while trying to connect:"
        print traceback.print_exc()
        sys.exit(2)

    sys.exit(0)

if __name__ == '__main__':
    main()