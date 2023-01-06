# py-tsbs-benchmark
Benchmark ingestion of the TSBS dataset into QuestDB via ILP using the `questdb` Python library and Pandas.

## Preparing QuestDB

Start a QuestDB instance.

```bash
questdb start
```

Then double-check that there's no valuable table called `'cpu'` as this is the
one that's going to be written to by the benchmark script.

The benchmark script assumes the instance is running on localhost on standard
ports: If the instance is remote or uses different ports you can pass the
`--host`, `--ilp-port` and `--http-port` arguments to the benchmark script.

## Running

### Box

You should be able to benchmark on any hardware with sufficient amounts of ram.
Expect high memory usage due to how the script creates the pandas dataframe in
memory.

The numbers included below are from the following setup:
* AMD 5950x
* 64G ram DDR4-3600
* 2TB Samsung 980 PRO
* Linux

For this specific box, I've tweaked the QuestDB
config as shown below. This is done to avoid the server instance overbooking
threads, given that we'll be also running the client on the same host.

```ini
# conf/server.conf
shared.worker.count=6
line.tcp.io.worker.count=6
```

If you're benchmarking on a separate machine then you shouldn't need any config
tweaks to get best performance.

Your milage may vary of course, but it's clear from the benchmarks below that
it's worth using the [`sender.dataframe()`](https://py-questdb-client.readthedocs.io/en/latest/api.html#questdb.ingress.Sender.dataframe) API and not looping through the
dataframe row by row in Python.

### Setup

```bash
poetry env use 3.10
poetry install
```

Note that each run will delete and re-create the `'cpu'` table.

### Single-threaded test

```bash
poetry run bench_pandas --send
```

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'row_count': 10000000,
     'scale': 4000,
     'seed': 2895858008286271758,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': None,
     'write_ilp': None}
Dropped table cpu
Created table cpu
Serialized:
  10000000 rows in 8.58s: 1.16 mil rows/sec.
  ILP Buffer size: 4652.22 MiB: 541.93 MiB/sec.
Sent:
  10000000 rows in 23.69s: 0.42 mil rows/sec.
  ILP Buffer size: 4652.22 MiB: 196.34 MiB/sec.
```

### Multi-threaded test

```bash
poetry run bench_pandas --send --workers 6
```

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'row_count': 10000000,
     'scale': 4000,
     'seed': 2818475543994300661,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': 6,
     'write_ilp': None}
Dropped table cpu
Created table cpu
Serialized:
  10000000 rows in 1.78s: 5.62 mil rows/sec.
  ILP Buffer size: 4652.37 MiB: 2616.29 MiB/sec.
Sent:
  10000000 rows in 8.90s: 1.12 mil rows/sec.
  ILP Buffer size: 4652.37 MiB: 522.94 MiB/sec.
```

### Full options

```bash
poetry run bench_pandas --help
```

## Choices made

We use the `'string[pyarrow]'` dtype in Pandas as it's one that allows us to
read the string column without needing to lock the GIL.

Compared to using a more conventional Python `str`-object `'O'` dtype Pandas
column type, this makes a significant different in the multi-threaded benchmark
as it enables parallelization, but makes little difference for a single-threaded
use case scenario where we've gone the [extra mile](https://github.com/questdb/py-questdb-client/tree/main/pystr-to-utf8)
to ensure fast Python string object to UTF-8 encoding performance.

## Sample of generated ILP messages

```
cpu,hostname=host_0,region=eu-west-1,datacenter=eu-west-1c,rack=22,os=Ubuntu15.10,arch=x86,team=LON,service=11,service_version=0,service_environment=staging usage_user=2.260713995474621,usage_system=0.7742634345475894,usage_idle=0.5433421797689806,usage_nice=0.0,usage_iowait=1.8872789915891544,usage_irq=0.5362196205980163,usage_softirq=0.7432769744844461,usage_steal=0.0,usage_guest=0.0,usage_guest_nice=1.2110585427526344 1451606400000000000
cpu,hostname=host_1,region=ap-northeast-1,datacenter=ap-northeast-1a,rack=53,os=Ubuntu15.10,arch=x86,team=NYC,service=1,service_version=0,service_environment=production usage_user=2.264693554570983,usage_system=0.5146965259325763,usage_idle=1.8878914216159703,usage_nice=0.0,usage_iowait=0.5884560303533308,usage_irq=0.42753305894872856,usage_softirq=0.801180194243782,usage_steal=0.8661127008514166,usage_guest=0.0,usage_guest_nice=0.5764978743281829 1451606410000000000
cpu,hostname=host_2,region=us-west-1,datacenter=us-west-1b,rack=29,os=Ubuntu15.10,arch=x86,team=SF,service=2,service_version=1,service_environment=production usage_user=2.6079664747344085,usage_system=0.42609358370322725,usage_idle=0.0016162253527125525,usage_nice=0.10596370190082907,usage_iowait=0.665106751584084,usage_irq=0.0,usage_softirq=0.6311393304729056,usage_steal=0.0,usage_guest=0.0,usage_guest_nice=1.2642526620101873 1451606420000000000
cpu,hostname=host_3,region=ap-southeast-2,datacenter=ap-southeast-2a,rack=68,os=Ubuntu15.10,arch=x64,team=NYC,service=12,service_version=1,service_environment=test usage_user=1.9812498570755634,usage_system=1.0573409130777713,usage_idle=0.6307345282945178,usage_nice=0.6577966205420174,usage_iowait=0.8692677309522628,usage_irq=0.0,usage_softirq=0.5188911519558501,usage_steal=0.46402279460697793,usage_guest=0.6656099875988695,usage_guest_nice=1.7476069678472128 1451606430000000000
cpu,hostname=host_4,region=us-east-1,datacenter=us-east-1e,rack=40,os=Ubuntu15.10,arch=x86,team=SF,service=11,service_version=1,service_environment=staging usage_user=2.5964868241838843,usage_system=0.0,usage_idle=1.2272999339697328,usage_nice=0.12023414661389953,usage_iowait=0.8395651302668741,usage_irq=0.0,usage_softirq=0.45434802944514724,usage_steal=0.0,usage_guest=0.0,usage_guest_nice=3.2814223881823787 1451606440000000000
cpu,hostname=host_5,region=eu-central-1,datacenter=eu-central-1b,rack=32,os=Ubuntu16.04LTS,arch=x86,team=SF,service=14,service_version=1,service_environment=staging usage_user=3.072615656127865,usage_system=0.0,usage_idle=1.3812601522351302,usage_nice=0.7655212714345465,usage_iowait=2.3434629262758166,usage_irq=0.3539595541407819,usage_softirq=0.0,usage_steal=2.9262011833188217,usage_guest=1.0922871015583087,usage_guest_nice=2.7897087006502304 1451606450000000000
cpu,hostname=host_6,region=us-west-2,datacenter=us-west-2c,rack=11,os=Ubuntu16.10,arch=x86,team=NYC,service=2,service_version=0,service_environment=test usage_user=2.8100880667177486,usage_system=1.0253398248948349,usage_idle=1.5919865749453264,usage_nice=0.0,usage_iowait=4.366890705367804,usage_irq=1.0361144031260785,usage_softirq=0.0,usage_steal=1.3542451068971073,usage_guest=2.8090962406357027,usage_guest_nice=5.027439036611597 1451606460000000000
cpu,hostname=host_7,region=ap-southeast-1,datacenter=ap-southeast-1a,rack=97,os=Ubuntu16.10,arch=x86,team=NYC,service=19,service_version=0,service_environment=staging usage_user=3.3933324938392984,usage_system=2.674165314702581,usage_idle=1.729746564369149,usage_nice=0.0,usage_iowait=2.6295278539977893,usage_irq=0.33325995202946646,usage_softirq=0.0,usage_steal=0.8629771143071407,usage_guest=3.5565038601505514,usage_guest_nice=4.295707748569857 1451606470000000000
cpu,hostname=host_8,region=eu-central-1,datacenter=eu-central-1b,rack=43,os=Ubuntu16.04LTS,arch=x86,team=SF,service=18,service_version=0,service_environment=production usage_user=2.3683820719125404,usage_system=3.1496636608187587,usage_idle=1.0714252817838013,usage_nice=0.0,usage_iowait=3.658575628441112,usage_irq=0.0,usage_softirq=0.0,usage_steal=0.9944564076833474,usage_guest=3.606177791932647,usage_guest_nice=5.665699532249171 1451606480000000000
cpu,hostname=host_9,region=sa-east-1,datacenter=sa-east-1b,rack=82,os=Ubuntu15.10,arch=x86,team=CHI,service=14,service_version=1,service_environment=staging usage_user=2.711560205310839,usage_system=2.92632821713108,usage_idle=1.6924636783124183,usage_nice=0.8654306023153091,usage_iowait=5.201435533195961,usage_irq=0.0,usage_softirq=1.7215318876485612,usage_steal=0.6839422702175311,usage_guest=3.1192465146389465,usage_guest_nice=5.414096713475799 1451606490000000000
```