# Sidewinder

[<img src="https://img.shields.io/badge/GitHub-prmoore77%2Fsidewinder-blue.svg?logo=Github">](https://github.com/prmoore77/sidewinder)
[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/repository/docker/prmoorevoltron/sidewinder/general)
[![sidewinder-ci](https://github.com/prmoore77/sidewinder/actions/workflows/ci.yml/badge.svg)](https://github.com/prmoore77/sidewinder/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/sidewinder-db)](https://pypi.org/project/sidewinder-db/)
[![PyPI version](https://badge.fury.io/py/sidewinder-db.svg)](https://badge.fury.io/py/sidewinder-db)
[![PyPI Downloads](https://img.shields.io/pypi/dm/sidewinder-db.svg)](https://pypi.org/project/sidewinder-db/)

Python-based Distributed Database

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
# Install Sidewinder-DB
pip install .
```

#### Alternative installation from PyPi
```shell
pip install sidewinder-db
```

### DuckDB CLI
Install DuckDB CLI version [0.6.1](https://github.com/duckdb/duckdb/releases/tag/v0.6.1) - and make sure the executable is on your PATH.

Platform Downloads:   
[Linux x86-64](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-amd64.zip)   
[Linux arm64 (aarch64)](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-aarch64.zip)   
[MacOS Universal](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-osx-universal.zip)   

## Generate source sample TPC-H (Scale Factor 1) data (only possible from repo currently, not PyPi package)
Note: If running on MacOS - you'll need to have [homebrew](https://brew.sh) installed, then install coreutils with:  
```brew install coreutils```

After that - you can create sample TPC-H source data for Scale Factor 1 (in parquet format) - run:
```
scripts/generate_tpch_data.sh 1
```

Next - you'll need to create a DuckDB database for the server (this is needed for the server to run queries that can't distribute) - run:
```
scripts/create_duckdb_database.sh 1
```

Next - you need to generate some shards - in this case we'll just generate 11 shards (we need an odd number for even distribution due to DuckDB's hash function):
```
pushd shard_generation
python -m build_shard_duckdb --shard-count=11 --source-data-path="../data/tpch/1" --output-data-path="../data/shards/tpch/1"
popd
```

## Run sidewinder locally - from root of repo (use --help option on the executables below for option details)
### Setup
Be sure to activate the virtual environment before running the executables
```shell
. ./venv/bin/activate
```
### 1) Server:
#### Open a terminal, then:
```sidewinder-server```

### 2) Worker:
#### Open another terminal, then start a single worker with command:
```sidewinder-worker```
##### Note: you can run up to 11 workers for this example configuration, to do that do this instead of starting a single-worker:
```bash
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
```sidewinder-client```

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
