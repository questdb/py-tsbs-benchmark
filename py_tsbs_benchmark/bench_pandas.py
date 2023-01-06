import questdb.ingress as qi
import numpy as np
import pandas as pd
import random
import time
import sys
import pprint
import textwrap
from concurrent.futures import ThreadPoolExecutor, Future
from numba import vectorize, float64

from .common import CpuTable


@vectorize([float64(float64, float64)])
def _clip_add(x, y):
    z = x + y
    # Clip to the 0 and 100 boundaries
    if z < 0.0:
        z = 0.0
    elif z > 100.0:
        z = 100.0
    return z


_REGIONS = {
    "us-east-1": [
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
        "us-east-1e"],
    "us-west-1": [
        "us-west-1a",
        "us-west-1b"],
    "us-west-2": [
        "us-west-2a",
        "us-west-2b",
        "us-west-2c"],
    "eu-west-1": [
        "eu-west-1a",
        "eu-west-1b",
        "eu-west-1c"],
    "eu-central-1": [
        "eu-central-1a",
        "eu-central-1b"],
    "ap-southeast-1": [
        "ap-southeast-1a",
        "ap-southeast-1b"],
    "ap-southeast-2": [
        "ap-southeast-2a",
        "ap-southeast-2b"],
    "ap-northeast-1": [
        "ap-northeast-1a",
        "ap-northeast-1c"],
    "sa-east-1": [
        "sa-east-1a",
        "sa-east-1b",
        "sa-east-1c"],
}


_REGION_KEYS = list(_REGIONS.keys())


_MACHINE_RACK_CHOICES = [
    str(n)
    for n in range(100)]


_MACHINE_OS_CHOICES = [
    "Ubuntu16.10",
    "Ubuntu16.04LTS",
    "Ubuntu15.10"]


_MACHINE_ARCH_CHOICES = [
    "x64",
    "x86"]


_MACHINE_TEAM_CHOICES = [
    "SF",
    "NYC",
    "LON",
    "CHI"]


_MACHINE_SERVICE_CHOICES = [
    str(n)
    for n in range(20)]


_MACHINE_SERVICE_VERSION_CHOICES = [
    str(n)
    for n in range(2)]


_MACHINE_SERVICE_ENVIRONMENT_CHOICES = [
    "production",
    "staging",
    "test"]


def gen_dataframe(seed, row_count, scale):
    rand, np_rand = random.Random(seed), np.random.default_rng(seed)

    def mk_hostname():
        repeated = [f'host_{n}' for n in range(scale)]
        repeat_count = row_count // scale + 1
        values = (repeated * repeat_count)[:row_count]
        return pd.Categorical(values)

    def rep_choice(choices):
        return rand.choices(choices, k=row_count)

    def mk_cpu_series():
        values = np_rand.normal(0, 1, row_count + 1)
        _clip_add.accumulate(values, out=values)
        return pd.Series(values[1:], dtype='float64')

    region = []
    datacenter = []
    for _ in range(row_count):
        reg = random.choice(_REGION_KEYS)
        region.append(reg)
        datacenter.append(rand.choice(_REGIONS[reg]))

    df = pd.DataFrame({
        'hostname': mk_hostname(),
        'region': pd.Categorical(region),
        'datacenter': pd.Categorical(datacenter),
        'rack': pd.Categorical(rep_choice(_MACHINE_RACK_CHOICES)),
        'os': pd.Categorical(rep_choice(_MACHINE_OS_CHOICES)),
        'arch': pd.Categorical(rep_choice(_MACHINE_ARCH_CHOICES)),
        'team': pd.Categorical(rep_choice(_MACHINE_TEAM_CHOICES)),
        'service': pd.Categorical(rep_choice(_MACHINE_SERVICE_CHOICES)),
        'service_version': pd.Categorical(
            rep_choice(_MACHINE_SERVICE_VERSION_CHOICES)),
        'service_environment': pd.Categorical(
            rep_choice(_MACHINE_SERVICE_ENVIRONMENT_CHOICES)),
        'usage_user': mk_cpu_series(),
		'usage_system': mk_cpu_series(),
		'usage_idle': mk_cpu_series(),
		'usage_nice': mk_cpu_series(),
		'usage_iowait': mk_cpu_series(),
		'usage_irq': mk_cpu_series(),
		'usage_softirq': mk_cpu_series(),
		'usage_steal': mk_cpu_series(),
		'usage_guest': mk_cpu_series(),
		'usage_guest_nice': mk_cpu_series(),
        'timestamp': pd.date_range('2016-01-01', periods=row_count, freq='10s'),
    })

    df.index.name = 'cpu'
    return df


def parse_args():
    seed = random.randrange(sys.maxsize)
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--row-count', type=int, default=10_000_000)
    parser.add_argument('--scale', type=int, default=4000)
    parser.add_argument('--seed', type=int, default=seed)
    parser.add_argument('--write-ilp', type=str, default=None)
    parser.add_argument('--shell', action='store_true', default=False)
    parser.add_argument('--send', action='store_true', default=False)
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--ilp-port', type=int, default=9009)
    parser.add_argument('--http-port', type=int, default=9000)
    parser.add_argument('--workers', type=int, default=None)
    parser.add_argument('--worker-chunk-row-count', type=int, default=10_000)
    parser.add_argument('--validation-query-timeout', type=float, default=120.0)
    parser.add_argument('--debug', action='store_true', default=False)
    return parser.parse_args()


def chunk_up_dataframe(df, chunk_row_count):
    dfs = []
    for i in range(0, len(df), chunk_row_count):
        dfs.append(df.iloc[i:i + chunk_row_count])
    return dfs


def assign_dfs_to_workers(dfs, workers):
    dfs_by_worker = [[] for _ in range(workers)]
    for i, df in enumerate(dfs):
        dfs_by_worker[i % workers].append(df)
    return dfs_by_worker


def sanity_check_split(df, dfs):
    df2 = pd.concat(dfs)
    assert len(df) == len(df2)
    assert df.equals(df2)


def sanity_check_split2(df, dfs_by_worker):
    df2 = pd.concat([
        df
        for dfs in dfs_by_worker
            for df in dfs])
    df2.sort_values(by='timestamp', inplace=True)
    assert len(df) == len(df2)
    assert df.equals(df2)


def chunk_up_by_worker(df, workers, chunk_row_count):
    dfs = chunk_up_dataframe(df, chunk_row_count)
    sanity_check_split(df, dfs)
    dfs_by_worker = assign_dfs_to_workers(dfs, workers)
    sanity_check_split2(df, dfs_by_worker)
    return dfs_by_worker


def serialize_one(args, df):
    buf = qi.Buffer()
    t0 = time.monotonic()
    buf.dataframe(df, at='timestamp')
    t1 = time.monotonic()
    elapsed = t1 - t0
    if args.write_ilp:
        if args.write_ilp == '-':
            print(buf)
        else:
            with open(args.write_ilp, 'w') as f:
                f.write(str(buf))
    row_speed = args.row_count / elapsed / 1_000_000.0
    print('Serialized:')
    print(
        f'  {args.row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    size_mb = len(buf) / 1024.0 / 1024.0
    throughput_mb = size_mb / elapsed
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')
    return len(buf)


def serialize_workers(args, df):
    dfs_by_worker = chunk_up_by_worker(
        df, args.workers, args.worker_chunk_row_count)
    bufs = [qi.Buffer() for _ in range(args.workers)]
    tpe = ThreadPoolExecutor(max_workers=args.workers)

    # Warm up the thread pool.
    tpe.map(lambda e: None, [None] * args.workers)

    if args.debug:
        repld = [False]
        import threading
        lock = threading.Lock()

        def serialize_dfs(buf, dfs):
            size = 0
            for df in dfs:
                try:
                    buf.dataframe(df, at='timestamp')
                except Exception as e:
                    with lock:
                        if not repld[0]:
                            import code
                            code.interact(local=locals())
                            repld[0] = True
                    raise e
                size += len(buf)
                buf.clear()
            return size
    else:
        def serialize_dfs(buf, dfs):
            size = 0
            for df in dfs:
                buf.dataframe(df, at='timestamp')
                size += len(buf)
                buf.clear()
            return size

    t0 = time.monotonic()
    futures = [
        tpe.submit(serialize_dfs, buf, dfs)
        for buf, dfs in zip(bufs, dfs_by_worker)]
    sizes = [fut.result() for fut in futures]
    t1 = time.monotonic()
    size = sum(sizes)
    elapsed = t1 - t0
    row_speed = args.row_count / elapsed / 1_000_000.0
    print('Serialized:')
    print(
        f'  {args.row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')
    return size


def send_one(args, df, size):
    with qi.Sender(args.host, args.ilp_port) as sender:
        t0 = time.monotonic()
        sender.dataframe(df, at='timestamp')
        sender.flush()
        t1 = time.monotonic()
    elapsed = t1 - t0
    row_speed = args.row_count / elapsed / 1_000_000.0
    print('Sent:')
    print(
        f'  {args.row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')


def send_workers(args, df, size):
    dfs_by_worker = chunk_up_by_worker(
        df, args.workers, args.worker_chunk_row_count)

    tpe = ThreadPoolExecutor(max_workers=args.workers)

    def connected_sender():
        sender = qi.Sender(args.host, args.ilp_port)
        sender.connect()
        return sender

    senders = [
        tpe.submit(connected_sender)
        for _ in range(args.workers)]
    senders: list[qi.Sender] = [f.result() for f in senders]

    def worker_job(sender, worker_dfs):
        with qi.Sender(args.host, args.ilp_port) as sender:
            t0 = time.monotonic()
            for df in worker_dfs:
                sender.dataframe(df, at='timestamp')
        t1 = time.monotonic()
        return t0, t1

    t0 = time.monotonic()
    futures: list[Future] = [
        tpe.submit(worker_job, sender, dfs)
        for sender, dfs in zip(senders, dfs_by_worker)]
    results: list[tuple[int, int]] = [
        f.result() for f in futures]
    t1 = time.monotonic()

    for sender in senders:
        sender.close()

    elapsed = t1 - t0
    row_speed = args.row_count / elapsed / 1_000_000.0
    print('Sent:')
    print(
        f'  {args.row_count} rows in {elapsed:.2f}s: '
        f'{row_speed:.2f} mil rows/sec.')
    throughput_mb = size / elapsed / 1024.0 / 1024.0
    size_mb = size / 1024.0 / 1024.0
    print(
        f'  ILP Buffer size: {size_mb:.2f} MiB: '
        f'{throughput_mb:.2f} MiB/sec.')


def main():
    args = parse_args()
    pretty_args = textwrap.indent(pprint.pformat(vars(args)), '    ')
    print(f'Running with params:\n{pretty_args}')

    cpu_table = CpuTable(args.host, args.http_port)

    if args.send:
        cpu_table.drop()
        cpu_table.create()

    df = gen_dataframe(args.seed, args.row_count, args.scale)

    if not args.workers:
        size = serialize_one(args, df)
    else:
        if args.workers < 1:
            raise ValueError('workers must be >= 1')
        size = serialize_workers(args, df)

    if args.shell:
        import code
        code.interact(local=locals())

    if args.send:
        if not args.workers:
            send_one(args, df, size)
        else:
            send_workers(args, df, size)

        cpu_table.block_until_rowcount(
            args.row_count, timeout=args.validation_query_timeout)
    else:
        print('Not sending. Use --send to send to server.')
