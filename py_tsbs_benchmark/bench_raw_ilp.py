import socket
import time
import struct
import pprint
import textwrap

from .common import CpuTable


class RawFileSender:
    def __init__(self, host, port, file_path, chunk_size=64 * 1024):
        self.host = host
        self.port = port
        self.file_path = file_path
        self.chunk_size = chunk_size

    def send(self):
        with open(self.file_path, 'rb') as f:
            buf = f.read()

        row_count = buf.count(b'\n')
        
        view = memoryview(buf)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        linger = 120  # seconds
        sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, linger))
        with sock:
            sock.connect((self.host, self.port))
            t0 = time.monotonic()
            for i in range(0, len(view), self.chunk_size):
                sock.sendall(view[i:i + self.chunk_size])
        t1 = time.monotonic()
        elapsed = t1 - t0
        row_speed = row_count / elapsed / 1_000_000.0
        size_mb = len(buf) / 1024.0 / 1024.0
        throughput_mb = size_mb / elapsed
        print('Sent:')
        print(
            f'  {row_count} rows in {elapsed:.2f}s: '
            f'{row_speed:.2f} mil rows/sec.')
        print(
            f'  {len(buf)} bytes in {elapsed:.2f}s: '
            f'{throughput_mb:.2f} MiB/sec.')

        return row_count


def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--ilp-port', type=int, default=9009)
    parser.add_argument('--http-port', type=int, default=9000)
    parser.add_argument('--chunk-size', type=int, default=64 * 1024)
    parser.add_argument('file_path', type=str)
    return parser.parse_args()


def main():
    args = parse_args()
    pretty_args = textwrap.indent(pprint.pformat(vars(args)), '    ')
    print(f'Running with params:\n{pretty_args}')

    cpu_table = CpuTable(args.host, args.http_port)
    cpu_table.drop()
    cpu_table.create()
    time.sleep(1)  # grace period.

    sender = RawFileSender(args.host, args.ilp_port, args.file_path)
    row_count = sender.send()

    cpu_table.block_until_rowcount(row_count)
