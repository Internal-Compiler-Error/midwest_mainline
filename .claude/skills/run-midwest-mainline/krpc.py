#!/usr/bin/env python3
"""A minimal KRPC (BEP 5) client for poking a running DHT node over UDP.

  krpc.py <host:port> ping
  krpc.py <host:port> get_peers <info-hash-hex>
  krpc.py <host:port> announce <info-hash-hex> <peer-port>   get_peers for a token, then announce_peer

Prints each response decoded, byte strings as hex unless they're printable.
"""
import os
import socket
import sys


def enc(x):
    if isinstance(x, int):
        return b"i%de" % x
    if isinstance(x, str):
        x = x.encode()
    if isinstance(x, bytes):
        return b"%d:" % len(x) + x
    if isinstance(x, list):
        return b"l" + b"".join(enc(i) for i in x) + b"e"
    if isinstance(x, dict):
        items = sorted((k.encode() if isinstance(k, str) else k, v) for k, v in x.items())
        return b"d" + b"".join(enc(k) + enc(v) for k, v in items) + b"e"
    raise TypeError(x)


def dec(b, i=0):
    c = b[i : i + 1]
    if c == b"i":
        j = b.index(b"e", i)
        return int(b[i + 1 : j]), j + 1
    if c in (b"l", b"d"):
        i += 1
        out = []
        while b[i : i + 1] != b"e":
            v, i = dec(b, i)
            out.append(v)
        return (out if c == b"l" else dict(zip(out[::2], out[1::2]))), i + 1
    j = b.index(b":", i)
    n = int(b[i:j])
    return b[j + 1 : j + 1 + n], j + 1 + n


def show(x):
    if isinstance(x, bytes):
        try:
            s = x.decode()
            if s.isprintable():
                return s
        except UnicodeDecodeError:
            pass
        return x.hex()
    if isinstance(x, list):
        return [show(i) for i in x]
    if isinstance(x, dict):
        return {show(k): show(v) for k, v in x.items()}
    return x


def query(sock, addr, q, args):
    msg = {"t": os.urandom(2), "y": "q", "q": q, "a": {"id": MY_ID, **args}}
    sock.sendto(enc(msg), addr)
    # the node pings a querier it doesn't know before it may join its table: skip that
    while True:
        reply, _ = sock.recvfrom(65536)
        reply, _ = dec(reply)
        if reply.get(b"t") == msg["t"] and reply.get(b"y") in (b"r", b"e"):
            break
    print(q, "->", show(reply))
    return reply


MY_ID = os.urandom(20)
host, port = sys.argv[1].rsplit(":", 1)
addr = (host, int(port))
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(5)
cmd = sys.argv[2]
if cmd == "ping":
    query(sock, addr, "ping", {})
elif cmd == "get_peers":
    query(sock, addr, "get_peers", {"info_hash": bytes.fromhex(sys.argv[3])})
elif cmd == "announce":
    info_hash = bytes.fromhex(sys.argv[3])
    token = query(sock, addr, "get_peers", {"info_hash": info_hash})[b"r"][b"token"]
    query(sock, addr, "announce_peer", {"info_hash": info_hash, "port": int(sys.argv[4]), "token": token, "implied_port": 0})
else:
    sys.exit(__doc__)
