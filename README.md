# Sidewinder

[<img src="https://img.shields.io/badge/GitHub-prmoore77%2Fsidewinder-blue.svg?logo=Github">](https://github.com/prmoore77/sidewinder)
[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/repository/docker/prmoorevoltron/sidewinder/general)
[![sidewinder-ci](https://github.com/prmoore77/sidewinder/actions/workflows/ci.yml/badge.svg)](https://github.com/prmoore77/sidewinder/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/sidewinder-db)](https://pypi.org/project/sidewinder-db/)
[![PyPI version](https://badge.fury.io/py/sidewinder-db.svg)](https://badge.fury.io/py/sidewinder-db)
[![PyPI Downloads](https://img.shields.io/pypi/dm/sidewinder-db.svg)](https://pypi.org/project/sidewinder-db/)

Python-based Distributed Database

### Note: Sidewinder is experimental - and is not intended for Production workloads. 

Sidewinder is a [Python](https://python.org)-based (with [asyncio](https://docs.python.org/3/library/asyncio.html)) Proof-of-Concept Distributed Database that distributes shards of data from the server to a number of workers to "divide and conquer" OLAP database workloads.

It consists of a server, workers, and a client (where you can run interactive SQL commands).

Sidewinder will NOT distribute queries which do not contain aggregates - it will run those on the server side. 

Sidewinder uses [Apache Arrow](https://arrow.apache.org) with [Websockets](https://websockets.readthedocs.io/en/stable/) for communication between the server, worker(s), and client(s).  

It uses [DuckDB](https://duckdb.org) as its SQL execution engine - and the PostgreSQL parser to understand how to combine results from distributed workers.

# Setup (to run locally)

## Install package

### Clone the repo
```shell
git clone https://github.com/prmoore77/sidewinder
```

### Python
Create a new Python 3.8+ virtual environment and install sidewinder-db with:
```shell
cd sidewinder

# Create the virtual environment
python3 -m venv ./venv

# Activate the virtual environment
. ./venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install Sidewinder-DB
pip install .
```

#### Alternative installation from PyPi
```shell
pip install sidewinder-db
```

## Bootstrap the environment by creating a sample TPC-H dataset with 11 shards
```shell
. ./venv/bin/activate
sidewinder-bootstrap --tpch-scale-factor=1 --shard-count=11
```

## Run sidewinder locally - from root of repo (use --help option on the executables below for option details)
### 1) Server:
#### Open a terminal, then:
```bash
. ./venv/bin/activate
sidewinder-server
```

### 2) Worker:
#### Open another terminal, then start a single worker with command:
```bash
. ./venv/bin/activate
sidewinder-worker
```
##### Note: you can run up to 11 workers for this example configuration, to do that do this instead of starting a single-worker:
```bash
. ./venv/bin/activate
for x in {1..11}:
do
  sidewinder-worker &
done
```

To kill the workers later - run:
```bash
kill $(jobs -p)
```

### 3) Client:
#### Open another terminal, then:
```
. ./venv/bin/activate
sidewinder-client
```

##### Then - while in the client - you can run a sample query that will distribute to the worker(s) (if you have at least one running) - example:
```SELECT COUNT(*) FROM lineitem;```
##### Note: if you are running less than 11 workers - your answer will only reflect n/11 of the data (where n is the worker count).  We will add delta processing at a later point...

##### A query that won't distribute (because it does not contain aggregates) - would be:
```SELECT * FROM region;```
##### or:
```SELECT * FROM lineitem LIMIT 5;```

##### Note: there are TPC-H queries in the [tpc-h_queries](tpc-h_queries) folder you can run...

##### To turn distributed mode OFF in the client:
```.set distributed = false;```

##### To turn summarization mode OFF in the client (so that sidewinder does NOT summarize the workers' results - this only applies to distributed mode):
```.set summarize = false;```

### Optional DuckDB CLI (use for data QA purposes, etc.)
Install DuckDB CLI version [0.8.0](https://github.com/duckdb/duckdb/releases/tag/v0.8.0) - and make sure the executable is on your PATH.

Platform Downloads:   
[Linux x86-64](https://github.com/duckdb/duckdb/releases/download/v0.8.0/duckdb_cli-linux-amd64.zip)   
[Linux arm64 (aarch64)](https://github.com/duckdb/duckdb/releases/download/v0.8.0/duckdb_cli-linux-aarch64.zip)   
[MacOS Universal](https://github.com/duckdb/duckdb/releases/download/v0.8.0/duckdb_cli-osx-universal.zip)   

