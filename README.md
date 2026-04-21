# Benchmarking Ingestion of Pandas into QuestDB

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🚀 Quick Start

For the impatient, here's how to run a basic benchmark:

```bash
# Clone and setup
git clone https://github.com/questdb/py-tsbs-benchmark.git
cd py-tsbs-benchmark
poetry install

# Start QuestDB (in separate terminal)
questdb start

# Run benchmark with data ingestion
poetry run bench_pandas --send --row-count 1000000
```

## Background
[QuestDB](https://questdb.io/) is our timeseries relational database with SQL
query support. We support a dedicate protocol (called
[ILP](https://questdb.io/docs/reference/api/ilp/overview/)) to ingest millions
of rows per second over TCP.

Many of our users are Python users who pre-process their data using
[Pandas](https://pandas.pydata.org/) dataframes and up until recently, however,
they would have to loop through the dataframes row-by-row in Python and would
see quite poor performance when doing this (mere thousands of rows per second).

We've recently introduced new functionality that iterates the dataframes in
native code achieving significant speedups.

## This Repo

This repository hosts code to benchmark the ingestion rate of
Pandas dataframes into QuestDB using the official
[`questdb`](https://py-questdb-client.readthedocs.io/en/latest/)
Python client library.

The benchmark reproduces and ingests the "dev ops" (a.k.a. 'cpu') dataset from
the [TSBS](https://github.com/timescale/tsbs) project over ILP into QuestDB.

The TSBS project is written in Go, and we replicate the same logic here in
Python: The generated data has the same columns, datatypes, cardinality etc.
Scroll to the end of this page to see a sample of generated data.

The data consists of:
* 10 SYMBOL columns (string columns with repeated values - i.e. interned)
* 10 DOUBLE columns (64-bit floats)
* 1 TIMESTAMP column (unix epoch nanoseconds, UTC)

To run these benchmarks, you will need:
* Modern hardware with multiple cores and enough RAM to hold a large Pandas dataset in memory
* **Python 3.10+** and [poetry](https://python-poetry.org/) for dependency management
* A recent version of **QuestDB** (v7.0.1 or later recommended)

You can follow through the setup and run commands below, or skip to see 
[benchmark results](#results) from our test runs.

## 📦 Prerequisites

### Python and Poetry

1. **Python 3.10+**: Download from [python.org](https://www.python.org/downloads/)
2. **Poetry**: Install using the official installer:
   ```bash
   # Linux/macOS/Windows (WSL)
   curl -sSL https://install.python-poetry.org | python3 -
   
   # Windows (PowerShell)
   (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
   ```

### QuestDB

Download and install QuestDB:
- **Docker**: `docker run -p 9000:9000 -p 9009:9009 questdb/questdb`
- **Binary**: Download from [questdb.io/get-questdb](https://questdb.io/get-questdb/)
- **Package managers**: `brew install questdb` (macOS) or see [installation guide](https://questdb.io/docs/get-started/docker/)

## Setup

### Python Environment and Dependencies

After cloning this repository:

```bash
# Clone the repository
git clone https://github.com/questdb/py-tsbs-benchmark.git
cd py-tsbs-benchmark

# Set up Python environment and install dependencies
poetry env use 3.10  # or python3.10 if poetry can't find it
poetry install

# Verify installation
poetry run bench_pandas --help
```

### Starting QuestDB

Start a QuestDB instance before running benchmarks:

```bash
# Method 1: Using Docker (recommended for testing)
docker run -p 9000:9000 -p 9009:9009 questdb/questdb

# Method 2: Using local installation
questdb start

# Method 3: Using Docker Compose (see docker-compose.yml if available)
docker-compose up questdb
```

**Verify QuestDB is running**: Open [http://localhost:9000](http://localhost:9000) in your browser.

## 🔧 Configuration

### Basic Usage

```bash
# Run serialization-only benchmark (no database)
poetry run bench_pandas --row-count 1000000

# Run full benchmark with database ingestion
poetry run bench_pandas --send --row-count 1000000

# Multi-threaded benchmark
poetry run bench_pandas --send --workers 4 --row-count 1000000

# Custom QuestDB connection
poetry run bench_pandas --send --host questdb.example.com --ilp-port 9009 --http-port 9000
```

### Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--row-count` | Number of rows to generate | 10,000,000 |
| `--scale` | Number of unique hostnames | 4,000 |
| `--workers` | Number of parallel threads | None (single-threaded) |
| `--send` | Send data to QuestDB | False (serialize only) |
| `--host` | QuestDB hostname | localhost |
| `--ilp-port` | ILP port | 9009 |
| `--http-port` | HTTP port | 9000 |
| `--op` | Operation type | dataframe |
| `--debug` | Enable debug logging | False |

## Running

### The hardware we used

The numbers included below are from the following setup:
* AMD 5950x
* 64G ram DDR4-3600
* 2TB Samsung 980 PRO
* Linux (kernel version: 5.19.0)
* Python 3.10.8
* QuestDB 7.0.1
* 12 threads for QuestDB server.
* 6 threads for the Python client in multi-threaded benchmarks.

### Configuration

For this specific hardware, we benchmark with a
[tweaked](https://questdb.io/docs/reference/api/ilp/tcp-receiver/#capacity-planning)
QuestDB config as shown below. This is done to avoid the server instance
overbooking threads, given that we'll be also running the client on the same
host.

```ini
# conf/server.conf
shared.worker.count=6
line.tcp.io.worker.count=6
cairo.wal.enabled.default=true
```

In addition, we've also enabled WAL tables as these give us better ingestion
performance (specifically ~1.3x faster for single-threaded, and ~1.6x faster
for multi-threaded gains in this specific benchmark suite and described
hardware).

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

By implementing the Pandas ingestion layer in native code, we're now ~28x faster
in single-threaded code and ~92x faster in multi-threaded code, including
database insert operations.

Our performance improvements for just serializing to an in-memory ILP buffer are
even better: Single-threaded serialization performance is ~58x faster and ~284x
faster when it's possible to serialize in parallel (when the Pandas column types
hold data directly and not through Python objects).

### Notes
  * Numbers are taken from the runs shown later in this same page.
  * Timings *exclude* the time taken to generate the sample Pandas dataframe.
  * NNx times faster calculated as FAST/SLOW using the MiB/s throughputs.

### Serialization to ILP in-memory buffer

*No network operations.*

<!--
https://quickchart.io/sandbox
{
  type: 'bar',
  data: {
    labels: ['df.iterrows() + sender.row()', 'sender.dataframe() - single thread', 'sender.dataframe() - 8 client threads'],
    datasets: [{
      label: 'Throughput MiB/s',
      data: [9.25, 543.04, 2635.69],
      borderColor: "#d14671",
      backgroundColor: "#d14671",
    }]
  }
}
-->
![chart](results/serialization.webp)

### Serialization, network send & data insertion into QuestDB

<!--
https://quickchart.io/sandbox
{
  type: 'bar',
  data: {
    labels: ['df.iterrows() + sender.row()', 'sender.dataframe() - single thread', 'sender.dataframe() - 8 client threads'],
    datasets: [{
      label: 'Throughput MiB/s',
      data: [9.13, 261.04, 843.18],
      borderColor: "#b1b5d3",
      backgroundColor: "#b1b5d3",
    }]
  }
}
-->
![chart](results/ingestion.webp)

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
     'seed': 6484453060204943748,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': None,
     'write_ilp': None}
Dropped table cpu
Created table cpu
Serialized:
  1000000 rows in 50.27s: 0.02 mil rows/sec.
  ILP Buffer size: 465.27 MiB: 9.25 MiB/sec.
Sent:
  1000000 rows in 50.98s: 0.02 mil rows/sec.
  ILP Buffer size: 465.27 MiB: 9.13 MiB/sec.
```

During profiling, we found out that this was dominated (over 90% of the time) by
iterating through the pandas dataframe and *not* the `.row()` method itself.

### Single-threaded test (New `.dataframe()` API)

The new [`sender.dataframe()`](https://py-questdb-client.readthedocs.io/en/latest/api.html#questdb.ingress.Sender.dataframe)
method resolves the performance problem by iterating through the data in native
code and is also easier to use from Python.

```python
with Sender('localhost', 9009) as sender:
    sender.dataframe(df, table_name='cpu', symbols=True, at='timestamp')
```

*Benchmarking code: `send_one` in [`py_tsbs_benchmark/bench_pandas.py`](py_tsbs_benchmark/bench_pandas.py).*

```bash
poetry run bench_pandas --send
```

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'py_row': False,
     'row_count': 10000000,
     'scale': 4000,
     'seed': 4803204514533752103,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': None,
     'write_ilp': None}
Table cpu does not exist
Created table cpu
Serialized:
  10000000 rows in 8.57s: 1.17 mil rows/sec.
  ILP Buffer size: 4652.50 MiB: 543.04 MiB/sec.
Sent:
  10000000 rows in 17.82s: 0.56 mil rows/sec.
  ILP Buffer size: 4652.50 MiB: 261.04 MiB/sec.
```

### Multi-threaded test (multithreaded use of `.dataframe()` API)

Since we release the Python GIL it's possible to also create multiple `sender`
objects and ingest in parallel. This means that the QuestDB database receives
the data [out of order, but the database deals with it](https://questdb.io/docs/concept/designated-timestamp#out-of-order-policy).

*Benchmarking code: `send_workers` in [`py_tsbs_benchmark/bench_pandas.py`](py_tsbs_benchmark/bench_pandas.py).*

```bash
poetry run bench_pandas --send --workers 8
```

```
Running with params:
    {'debug': False,
     'host': 'localhost',
     'http_port': 9000,
     'ilp_port': 9009,
     'py_row': False,
     'row_count': 10000000,
     'scale': 4000,
     'seed': 1038685014730277296,
     'send': True,
     'shell': False,
     'validation_query_timeout': 120.0,
     'worker_chunk_row_count': 10000,
     'workers': 8,
     'write_ilp': None}
Dropped table cpu
Created table cpu
Serialized:
  10000000 rows in 1.77s: 5.66 mil rows/sec.
  ILP Buffer size: 4652.60 MiB: 2635.69 MiB/sec.
Sent:
  10000000 rows in 5.52s: 1.81 mil rows/sec.
  ILP Buffer size: 4652.60 MiB: 843.18 MiB/sec.
```

### Full options

```bash
poetry run bench_pandas --help
```

## The `.dataframe()` method inner workings

The TL;DR of how we achieve these numbers is by avoiding calling the Python
interpreter within the send loop whenever possible.

Data in pandas is (usually) laid out as columns of contiguous memory.
Each column (series) is accessible either as a
[numpy array](https://numpy.org/), itself accessible via the
[Python Buffer protocol](https://docs.python.org/3/c-api/buffer.html), or
accessible as an Apache Arrow array via their
[C data interface](https://arrow.apache.org/docs/format/CDataInterface.html).
We've done some experimentation to figure
out which Pandas datatypes are best suited to either access pattern and go from
there. We try to avoid copies whenever possible (almost always possible).

We loop the buffers for the series in Cython (which compiles down to C and
eventually native code) and call our serialization functions which are written
in Rust and themselves have a C API. A bit of inlining and link time
optimization and we can get good numbers.

As a bonus, this approach also allows us to release the Python GIL and
parallelize across threads for customers that need that little bit of extra
performance.

If you're interested in the actual implementation, it lives here:
https://github.com/questdb/py-questdb-client/blob/main/src/questdb/dataframe.pxi

## Pandas Dataframe String Column Choice

We use the `'string[pyarrow]'` dtype in Pandas as it allows us to
read the string column without needing to lock the GIL.

Compared to using a more conventional Python `str`-object `'O'` dtype Pandas
column type, this makes a significant difference in the multi-threaded benchmark
as it enables parallelization, but makes little difference for a single-threaded
use case scenario where we've gone the
[extra mile](https://github.com/questdb/py-questdb-client/tree/main/pystr-to-utf8)
to ensure fast Python `str` object to UTF-8 encoding by handling the interal
UCS-1, UCS-2 and UCS-4 representations in a small helper library in Rust.

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

## 🧪 Testing

Run the test suite to verify everything is working:

```bash
# Run all tests
poetry run python -m pytest tests/ -v

# Run tests with coverage
poetry run python -m pytest tests/ --cov=py_tsbs_benchmark --cov-report=html

# Run specific test modules
poetry run python -m pytest tests/test_common.py -v
poetry run python -m pytest tests/test_bench_pandas.py -v
```

## 🤝 Contributing

We welcome contributions! This project is designed to be beginner-friendly for those 
wanting to contribute to open-source Python projects.

### Getting Started with Development

1. **Fork and clone** the repository
2. **Set up development environment**:
   ```bash
   poetry install --dev  # Install with development dependencies
   poetry run pre-commit install  # Set up git hooks (if available)
   ```
3. **Run tests** to ensure everything works:
   ```bash
   poetry run python -m pytest tests/ -v
   ```

### Development Guidelines

- **Code Style**: Follow PEP 8, use type hints where possible
- **Documentation**: Add docstrings to all functions and classes
- **Testing**: Write unit tests for new functionality
- **Error Handling**: Use appropriate try-catch blocks and logging
- **Commit Messages**: Use clear, descriptive commit messages in English

### Suggested Areas for Contribution

1. **Performance optimizations** in data generation or serialization
2. **Additional benchmark metrics** (memory usage, latency percentiles)
3. **Support for different data types** beyond the current TSBS schema
4. **Improved error handling** and recovery mechanisms
5. **Documentation improvements** and examples
6. **CI/CD pipeline** setup and automation

### Submitting Changes

1. Create a feature branch: `git checkout -b feature/your-improvement`
2. Make your changes with proper tests and documentation
3. Run the test suite: `poetry run python -m pytest tests/ -v`
4. Commit your changes: `git commit -m "feat: add your improvement"`
5. Push and create a Pull Request with a clear description

### Code of Conduct

Please be respectful and constructive in all interactions. This project follows 
the [Contributor Covenant](https://www.contributor-covenant.org/).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙋‍♀️ Support

- **Documentation**: [QuestDB Documentation](https://questdb.io/docs/)
- **Issues**: [GitHub Issues](https://github.com/questdb/py-tsbs-benchmark/issues)
- **Community**: [QuestDB Slack](https://slack.questdb.io/)
- **Python Client**: [py-questdb-client docs](https://py-questdb-client.readthedocs.io/)
