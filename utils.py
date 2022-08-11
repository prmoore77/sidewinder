import asyncio
import base64
from functools import wraps

import pyarrow
import duckdb
from config import logger
import os


# taken from: https://donghao.org/2022/01/20/how-to-get-the-number-of-cpu-cores-inside-a-container/
def get_cpu_limit():
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as fp:
            cfs_quota_us = int(fp.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as fp:
            cfs_period_us = int(fp.read())
        container_cpus = cfs_quota_us // cfs_period_us
        # For physical machine, the `cfs_quota_us` could be '-1'
        cpus = os.cpu_count() if container_cpus < 1 else container_cpus
    except Exception:
        cpus = os.cpu_count()

    return cpus


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


def combine_bytes_results(result_bytes_list, summary_query, duckdb_threads) -> bytes:
    table_list = []
    for result_bytes in result_bytes_list:
        table_list.append(get_dataframe_from_bytes(bytes_value=result_bytes))

    combined_result = pyarrow.concat_tables(tables=table_list)

    con = duckdb.connect(database=':memory:')
    if duckdb_threads:
        con.execute(f"PRAGMA threads={duckdb_threads};")

    logger.info(msg=f"Running summarization query: '{summary_query}'")
    summarized_result = con.execute(summary_query).fetch_arrow_table()

    return get_dataframe_bytes(df=summarized_result)
