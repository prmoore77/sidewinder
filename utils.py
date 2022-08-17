import asyncio
import base64
from functools import wraps
import psutil

import pyarrow
import duckdb
from config import logger
import os


# taken from: https://donghao.org/2022/01/20/how-to-get-the-number-of-cpu-cores-inside-a-container/
def get_cpu_count():
    if os.path.isfile('/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as fp:
            cfs_quota_us = int(fp.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as fp:
            cfs_period_us = int(fp.read())
        container_cpus = cfs_quota_us // cfs_period_us
        # For physical machine, the `cfs_quota_us` could be '-1'
        cpus = os.cpu_count() if container_cpus < 1 else container_cpus
    else:
        cpus = os.cpu_count()

    return cpus


def get_memory_limit():
    if os.path.isfile('/sys/fs/cgroup/memory/memory.limit_in_bytes'):
        with open('/sys/fs/cgroup/memory/memory.limit_in_bytes') as limit:
            memory_limit = int(limit.read())
    else:
        memory_limit = psutil.virtual_memory().total

    return memory_limit


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def get_dataframe_from_bytes(bytes_value: bytes) -> pyarrow.Table:
    return pyarrow.ipc.open_stream(bytes_value).read_all()


def get_dataframe_bytes(df: pyarrow.Table) -> bytes:
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, df.schema) as writer:
        writer.write(df)
    buf = sink.getvalue()
    return buf.to_pybytes()


def get_dataframe_results_as_base64_str(df: pyarrow.Table) -> str:
    return base64.b64encode(get_dataframe_bytes(df)).decode()


def combine_bytes_results(result_bytes_list, summary_query, duckdb_threads, duckdb_memory_limit, summarize_results: bool = True) -> bytes:
    table_list = []
    for result_bytes in result_bytes_list:
        table_list.append(get_dataframe_from_bytes(bytes_value=result_bytes))

    combined_result = pyarrow.concat_tables(tables=table_list)

    con = duckdb.connect(database=':memory:')
    if duckdb_threads:
        con.execute(f"PRAGMA threads={duckdb_threads};")
    if duckdb_memory_limit:
        con.execute(f"PRAGMA memory_limit='{duckdb_memory_limit}b';")

    if summarize_results:
        logger.info(msg=f"Running summarization query: '{summary_query}'")
        summarized_result = con.execute(summary_query).fetch_arrow_table()
    else:
        logger.warning(msg=f"NOT running summarization query - b/c client summarization mode is False...'")
        summarized_result = combined_result

    return get_dataframe_bytes(df=summarized_result)


def duckdb_execute(con, sql: str):
    logger.debug(msg=f"Executing DuckDB SQL:\n{sql}")
    return con.execute(sql)


def run_query(database_file, sql, duckdb_threads, duckdb_memory_limit) -> bytes:
    con = duckdb.connect(database=database_file, read_only=True)
    if duckdb_threads:
        duckdb_execute(con, sql=f"PRAGMA threads={duckdb_threads}")
    if duckdb_memory_limit:
        duckdb_execute(con, sql=f"PRAGMA memory_limit='{duckdb_memory_limit}b'")

    logger.info(msg=f"Running server-side query: '{sql}'")

    query_result = con.execute(sql).fetch_arrow_table()

    return get_dataframe_bytes(df=query_result)
