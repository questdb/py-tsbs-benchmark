# py-tsbs-benchmark
This repository hosts code to benchmark the ingestion rate of
[Pandas](https://pandas.pydata.org/) dataframes into
[QuestDB](https://questdb.io/) using the
[`questdb`](https://py-questdb-client.readthedocs.io/en/latest/)
Python client library.

It replicates and inserts the "dev ops" (a.k.a. 'cpu') dataset from the
[TSBS](https://github.com/timescale/tsbs) project.

The TSBS project is written in Go, and we replicate the same logic here in
Python: The generated data has the same columns, datatypes, cardinality etc.
Scroll to the end of this page to see a sample of generated data.

To run these benchmarks, you will need:
* Modern hardware with multiple cores and enough
ram to hold a large Pandas dataset in memory.
* Python 3.10 and [poetry](https://python-poetry.org/).
* and a recent version of QuestDB.

You can follow through the setup and the run commands or just scroll down to see
numbers from our benchmark runs.

## Setup

### Python Client

After cloning this git repo:

```bash
poetry env use 3.10
poetry install
```

Note that each benchmark run will delete and re-create the `'cpu'` table.

### Preparing QuestDB

Start a QuestDB instance.

```bash
questdb start
```

## Running

### The hardware we used

The numbers included below are from the following setup:
* AMD 5950x
* 64G ram DDR4-3600
* 2TB Samsung 980 PRO
* Linux (kernel version: 6.0.6)
* Python 3.10.8
* QuestDB 6.6.1
* 12 threads for QuestDB server.
* 6 threads for the Python client in multi-threaded benchmarks.

### Configuration

For this specific hardware, we benchmark with tweaked QuestDB
config as shown below. This is done to avoid the server instance overbooking
threads, given that we'll be also running the client on the same host.

```ini
# conf/server.conf
shared.worker.count=6
line.tcp.io.worker.count=6
```

If your benchmarking client and QuestDB server are on separate machines then you
shouldn't need any config tweaks to get the best performance.

The benchmark script assumes that the instance is running on localhost on standard
ports: If the instance is remote or uses different ports you can pass the
`--host`, `--ilp-port` and `--http-port` arguments to the benchmark script
shown later.

Your milage may vary of course, but it's clear from the benchmarks below that
it's worth using the [`sender.dataframe()`](https://py-questdb-client.readthedocs.io/en/latest/api.html#questdb.ingress.Sender.dataframe)
API and not looping through the dataframe row by row in Python.

## Results

By implementing the Pandas ingestion layer in native code, we're now ~20x faster
in single-threaded code and ~60x faster in multi-threaded code, including
database insert operations.

Our performance improvements for just serializing to an in-memory ILP buffer are
even better: Single threaded serialization performance is ~60x faster and ~290x
faster when it's possible to serialize in parallel (when the Pandas column types
hold data directly and not through Python objects).

### Notes
  * Numbers are taken from the runs shown later in this same page.
  * Timings *exclude* the time taken to generate the sample Pandas dataframe.

### Serialization to ILP in-memory buffer

*No network operations.*

<!--
https://quickchart.io/sandbox
{
  type: 'bar',
  data: {
    labels: ['df.iterrows() + sender.row()', 'sender.dataframe() - single thread', 'sender.dataframe() - 6 client threads'],
    datasets: [{
      label: 'Throughput MiB/s',
      data: [8.96, 541.93, 2616.29],
      borderColor: "#d14671",
      backgroundColor: "#d14671",
    }]
  }
}
-->
![chart](https://quickchart.io/chart?w=800&h=400&v=2.9.4&c=%7B%0A%20%20type%3A%20%27bar%27%2C%0A%20%20data%3A%20%7B%0A%20%20%20%20labels%3A%20%5B%27df.iterrows()%20%2B%20sender.row()%27%2C%20%27sender.dataframe()%20-%20single%20thread%27%2C%20%27sender.dataframe()%20-%206%20client%20threads%27%5D%2C%0A%20%20%20%20datasets%3A%20%5B%7B%0A%20%20%20%20%20%20label%3A%20%27Throughput%20MiB%2Fs%27%2C%0A%20%20%20%20%20%20data%3A%20%5B8.96%2C%20541.93%2C%202616.29%5D%2C%0A%20%20%20%20%20%20borderColor%3A%20%22%23d14671%22%2C%0A%20%20%20%20%20%20backgroundColor%3A%20%22%23d14671%22%2C%0A%20%20%20%20%7D%5D%0A%20%20%7D%0A%7D)

### Serialization, network send & data insertion into QuestDB

<!--
https://quickchart.io/sandbox
{
  type: 'bar',
  data: {
    labels: ['df.iterrows() + sender.row()', 'sender.dataframe() - single thread', 'sender.dataframe() - 6 client threads'],
    datasets: [{
      label: 'Throughput MiB/s',
      data: [8.99, 196.34, 522.94],
      borderColor: "#b1b5d3",
      backgroundColor: "#b1b5d3",
    }]
  }
}
-->
![chart](https://quickchart.io/chart?w=800&h=400&v=2.9.4&c=%7B%0A%20%20type%3A%20%27bar%27%2C%0A%20%20data%3A%20%7B%0A%20%20%20%20labels%3A%20%5B%27df.iterrows()%20%2B%20sender.row()%27%2C%20%27sender.dataframe()%20-%20single%20thread%27%2C%20%27sender.dataframe()%20-%206%20client%20threads%27%5D%2C%0A%20%20%20%20datasets%3A%20%5B%7B%0A%20%20%20%20%20%20label%3A%20%27Throughput%20MiB%2Fs%27%2C%0A%20%20%20%20%20%20data%3A%20%5B8.99%2C%20196.34%2C%20522.94%5D%2C%0A%20%20%20%20%20%20borderColor%3A%20%22%23b1b5d3%22%2C%0A%20%20%20%20%20%20backgroundColor%3A%20%22%23b1b5d3%22%2C%0A%20%20%20%20%7D%5D%0A%20%20%7D%0A%7D)

### Without Pandas Support (pre-existing `.row()` API)

Before Pandas support provided by the new
[`questdb>=1.1.0`](https://pypi.org/project/questdb/) Python client,
one had to iterate a Pandas dataframe row by row:

```python
with Sender('localhost', 9009) as sender:
    for _index, row in df.iterrows():
        sender.row(
            'cpu',
            symbols={
                'hostname': row['hostname'],
                'region': row['region'],
                'datacenter': row['datacenter'],
                'rack': row['rack'],
                'os': row['os'],
                'arch': row['arch'],
                'team': row['team'],
                'service': row['service'],
                'service_version': row['service_version'],
                'service_environment': row['service_environment']},
            columns={
                'usage_user': row['usage_user'],
                'usage_system': row['usage_system'],
                'usage_idle': row['usage_idle'],
                'usage_nice': row['usage_nice'],
                'usage_iowait': row['usage_iowait'],
                'usage_irq': row['usage_irq'],
                'usage_softirq': row['usage_softirq'],
                'usage_steal': row['usage_steal'],
                'usage_guest': row['usage_guest'],
                'usage_guest_nice': row['usage_guest_nice']},
            at=TimestampNanos(row['timestamp'].value))
```

This was *very* slow.

```
poetry run bench_pandas --py-row --send --row-count 1000000
```

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'py_row': True,
     'row_count': 1000000,
     'scale': 4000,
     'seed': 6568188686568556488,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': None,
     'write_ilp': None}
Dropped table cpu
Created table cpu
Serialized:
  1000000 rows in 51.94s: 0.02 mil rows/sec.
  ILP Buffer size: 465.19 MiB: 8.96 MiB/sec.
Sent:
  1000000 rows in 52.30s: 0.02 mil rows/sec.
  ILP Buffer size: 465.19 MiB: 8.89 MiB/sec.
```

In profiling we found out that this was dominated (over 90% of time) by
iterating through the pandas dataframe and *not* the `.row()` method itself.

The new [`sender.dataframe()`](https://py-questdb-client.readthedocs.io/en/latest/api.html#questdb.ingress.Sender.dataframe)
method resolves the performance problem by iterating through the data in native
code and is also easier to use from Python.

```python
with Sender('localhost', 9009) as sender:
    sender.dataframe(df, table_name='cpu', symbols=True, at='timestamp')
```

### Single-threaded test (New `.dataframe()` API)

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

### Multi-threaded test (multithreaded use of `.dataframe()` API)

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

We use the `'string[pyarrow]'` dtype in Pandas as it allows us to
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
