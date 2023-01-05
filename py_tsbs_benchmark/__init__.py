import questdb.ingress as qi
import numpy as np
import pandas as pd
import random
import time
from numba import vectorize, float64


@vectorize([float64(float64, float64)])
def _bounded_add(x, y):
    z = x + y
    # Bounce the walk off the 0 and 100 boundaries
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
    rand, np_rand = (random.Random(seed), np.random.default_rng(seed)) \
        if seed is not None \
        else (random.Random(), np.random.default_rng())

    def mk_hostname():
        repeated = [f'host_{n}' for n in range(scale)]
        repeat_count = row_count // scale + 1
        values = (repeated * repeat_count)[:row_count]
        return pd.Categorical(values)

    def rep_choice(choices):
        return rand.choices(choices, k=row_count)

    def mk_cpu_series():
        values = np_rand.normal(0, 1, row_count + 1)
        _bounded_add.accumulate(values, out=values)
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--row-count', type=int, default=10_000_000)
    parser.add_argument('--scale', type=int, default=4000)
    parser.add_argument('--seed', type=int, default=None)
    parser.add_argument('--write-ilp', type=str, default=None)
    parser.add_argument('--send', action='store_true', default=False)
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=9009)
    return parser.parse_args()


def serialize(args, df):
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


def main():
    args = parse_args()
    print(f'Running with params: {vars(args)}')
    df = gen_dataframe(args.seed, args.row_count, args.scale)
  
    size = serialize(args, df)

    if args.send:
        with qi.Sender(args.host, args.port) as sender:
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
    else:
        print('Not sending. Use --send to send to server.')
