#!/usr/bin/python

import socket
import struct
import sys

sys.argv.pop(0)

mask_hex_string = '0xffffff80'
mask = int(mask_hex_string, 16)


def ip2int(addr):
  return struct.unpack("!I", socket.inet_aton(addr))[0]


def int2ip(addr):
  return socket.inet_ntoa(struct.pack("!I", addr))


for hostname in sys.argv:
  try:
    ip = socket.gethostbyname(hostname)
    result = int2ip(ip2int(ip) & mask)
    print '/{0}'.format(result)
  except:
    print '/default-rack'
