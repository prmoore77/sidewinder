# sidewinder
Skunkworks Distributed Database

Sidewinder is a Proof-of-Concept Distributed Database that distributes shards of data from the server to  
a number of workers to "divide and conquer" OLAP database workloads.

It consists of a server, workers, and a client (where you can run interactive SQL commands).

Sidewinder will NOT distribute queries which do not contain aggregates - it will run those on the server side. 

Sidewinder uses Apache Arrow with Websockets for communication between the server, worker(s), and client(s).  

It uses DuckDB as its SQL execution engine - and the PostgreSQL parser to understand how to combine results from  
distributed workers.

# Setup (to run locally)

## Install requirements
Create a new Python 3.8+ virtual environment - from the root of the repo: install the requirements with:
```shell
pip install -r requirements.txt
```

## Get source sample TPC-H Scale Factor 1 Data (about a 295MB download)
The sample data is in an Amazon Cloud S3 bucket (owned by Voltron Data) - and requires authentication.
Authenticate to Amazon Cloud - then select account: "voltrondata-developers"
Open a terminal - then set your Environment variables for use with the cli
- see: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html?icmpid=docs_sso_user_portal

Note: If running on MacOS - you'll need to have homebrew installed, then install coreutils with:  
```brew install coreutils```

After that - you can get a sample TPC-H source DuckDB database - run:
```
scripts/copy_tpch_data.sh 1
scripts/create_duckdb_database.sh 1
```

## Run sidewinder locally (from root of repo)
### 1) Server:
#### Open a terminal, then:
```python -m server```

### 2) Worker:
#### Open another terminal, then:
```python -m worker```
##### Note:  you can run up to 10 workers in the default configuration... 

### 3) Client:
#### Open another terminal, then:
```python -m client```

##### Then - while in the client - you can run a sample query - example:
```SELECT COUNT(*) FROM lineitem```

##### To turn distributed mode OFF in the client:
```.set distributed = false;```
