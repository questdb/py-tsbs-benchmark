import socket
import time
import struct


class RawFileSender:
    def __init__(self, host, port, file_path, chunk_size=64 * 1024):
        self.host = host
        self.port = port
        self.file_path = file_path
        self.chunk_size = chunk_size

    def send(self):
        with open(self.file_path, 'rb') as f:
            buf = f.read()
        
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
        size_mb = len(buf) / 1024.0 / 1024.0
        throughput_mb = size_mb / elapsed
        print('Sent:')
        print(
            f'  {len(buf)} bytes in {elapsed:.2f}s: '
            f'{throughput_mb:.2f} MiB/sec.')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9009)
    parser.add_argument('--chunk-size', type=int, default=64 * 1024)
    parser.add_argument('file_path', type=str)
    args = parser.parse_args()
    sender = RawFileSender(args.host, args.port, args.file_path)
    sender.send()
