# coding: utf-8

# Copyright Luna Technology 2015
# Matthieu Riviere <mriviere@luna-technology.com>

import socket
import sys


def main(pid):
    socket_path = '/tmp/smbproxy-%d.sock' % (pid,)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)
    sock.sendall('SHUTDOWN\n')
    sock.recv(4096)


if __name__ == '__main__':
    main(int(sys.argv[1]))