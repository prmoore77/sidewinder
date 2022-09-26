# Sidewinder
Python-based Distributed Database

Sidewinder is a [Python](https://python.org)-based (with [asyncio](https://docs.python.org/3/library/asyncio.html)) Proof-of-Concept Distributed Database that distributes shards of data from the server to a number of workers to "divide and conquer" OLAP database workloads.

It consists of a server, workers, and a client (where you can run interactive SQL commands).

Sidewinder will NOT distribute queries which do not contain aggregates - it will run those on the server side. 

Sidewinder uses [Apache Arrow](https://arrow.apache.org) with [Websockets](https://websockets.readthedocs.io/en/stable/) for communication between the server, worker(s), and client(s).  

It uses [DuckDB](https://duckdb.org) as its SQL execution engine - and the PostgreSQL parser to understand how to combine results from distributed workers.

# Setup (to run locally)

## Install requirements
Create a new Python 3.8+ virtual environment - from the root of the repo: install the requirements with:
```shell
pip install -r requirements.txt
```

## Get source sample TPC-H (Scale Factor 1) data (about a 295MB download)
The sample data is in an Amazon Cloud S3 bucket (owned by Voltron Data) - and requires authentication.
Authenticate to Amazon Cloud - then select account: "voltrondata-developers".
Open a terminal - then set your Environment variables for use with the cli
- see: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html?icmpid=docs_sso_user_portal

Note: If running on MacOS - you'll need to have [homebrew](https://brew.sh) installed, then install coreutils with:  
```brew install coreutils```

After that - you can create a sample TPC-H source DuckDB database - for Scale Factor 1 parquet data - run:
```
scripts/generate_tpch_data.sh 1
```

Next - you'll need to create a DuckDB database for the server (this is needed for the server to run queries that can't distribute) - run:
```
scripts/create_duckdb_database.sh 1
```

Next - you need to generate some shards - in this case we'll just generate 11 shards (we need an odd number for even distribution due to DuckDB's hash function):
```
cd shard_generation
python -m build_shard_duckdb --shard-count=11 --source-data-path="../data/tpch/1" --output-data-path="../data/shards/tpch/1"
cd ..
```

## Run sidewinder locally (from root of repo)
### 1) Server:
#### Open a terminal, then:
```python -m server```

### 2) Worker:
#### Open another terminal, then:
```python -m worker```
##### Note: you can run up to 11 workers for this example configuration... 

### 3) Client:
#### Open another terminal, then:
```python -m client```

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
