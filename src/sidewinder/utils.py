import asyncio
import base64
import hashlib
import os
import re
import shutil
from functools import wraps
from typing import List, Tuple

import boto3
import duckdb
import psutil
import pyarrow
import requests
from pyarrow import parquet as pq
from botocore.config import Config
from codetiming import Timer
from munch import Munch
from sidewinder.config import logger
from sidewinder.constants import SHARD_URL_EXPIRATION_SECONDS, TIMER_TEXT, PARQUET_RESULT_TYPE, ARROW_RESULT_TYPE


# taken from: https://donghao.org/2022/01/20/how-to-get-the-number-of-cpu-cores-inside-a-container/
def get_cpu_count():
    if os.path.isfile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"):
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


def get_dataframe_from_ipc_bytes(bytes_value: bytes) -> pyarrow.Table:
    return pyarrow.ipc.open_stream(bytes_value).read_all()


def get_dataframe_from_parquet_bytes(bytes_value: bytes) -> pyarrow.Table:
    return pq.read_table(source=pyarrow.BufferReader(pyarrow.py_buffer(bytes_value)))


def get_dataframe_ipc_bytes(df: pyarrow.Table) -> bytes:
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, df.schema) as writer:
        writer.write(df)
    buf = sink.getvalue()
    return buf.to_pybytes()


def get_dataframe_results_as_ipc_base64_str(df: pyarrow.Table) -> str:
    return base64.b64encode(get_dataframe_ipc_bytes(df)).decode()


def combine_bytes_results(result_bytes_list, summary_query, duckdb_threads, duckdb_memory_limit,
                          summarize_results: bool = True) -> bytes:
    table_list = []
    for result_bytes in result_bytes_list:
        if result_bytes.get("result_type") == ARROW_RESULT_TYPE:
            table_list.append(get_dataframe_from_ipc_bytes(bytes_value=result_bytes.get("results")))
        elif result_bytes.get("result_type") == PARQUET_RESULT_TYPE:
            table_list.append(get_dataframe_from_parquet_bytes(bytes_value=result_bytes.get("results")))

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

    return get_dataframe_ipc_bytes(df=summarized_result)


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

    return get_dataframe_ipc_bytes(df=query_result)


async def parse_s3_url(s3_url: str):
    bucket_name = s3_url.split("/")[2]
    file_path = "/".join(s3_url.split("/")[3:])

    return bucket_name, file_path


async def download_file(src: str, dst: str):
    # Don't print the signature to the log (for security reasons)
    with Timer(name=f"Downloading shard file - from url: {src.split('?')[0]} to path: {dst}",
               text=TIMER_TEXT,
               initial_text=True,
               logger=logger.info
               ):
        req = requests.get(url=src, allow_redirects=True)
        req.raise_for_status()
        with open(file=dst, mode="wb") as dst_file:
            dst_file.write(req.content)


async def get_s3_file_hash(bucket_name: str, key: str) -> Tuple[str, str]:
    s3_client = boto3.client("s3")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor=None, func=lambda: s3_client.head_object(Bucket=bucket_name, Key=key))

    s3_file_hash = result["ETag"].strip('"')
    s3_file_name = f"s3://{bucket_name}/{key}"

    return s3_file_name, s3_file_hash


async def get_s3_shard_files(shard_data_path) -> List[Munch]:
    s3_client = boto3.client("s3")

    bucket_name, file_path = await parse_s3_url(s3_url=shard_data_path)

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=file_path)

    s3_files = []
    for page in pages:
        files = page['Contents']

        # Call the get_s3_file_hash function asynchronously for better performance/concurrency
        for result_tuple in asyncio.as_completed([get_s3_file_hash(bucket_name=bucket_name,
                                                                   key=file['Key']
                                                                   )
                                                 for file in files
                                                 if file_path in file['Key'] and re.search(pattern=r"\.tar\.zst$", string=file['Key'])
                                              ]):
            shard_file_tuple = await result_tuple
            s3_files.append(Munch(shard_file_name=shard_file_tuple[0],
                                  shard_file_hash=shard_file_tuple[1]
                                  )
                            )

    return s3_files


async def pre_sign_shard_url(shard_file_url: str) -> str:
    if re.search(r"^s3://", shard_file_url):
        s3_client = boto3.client("s3", config=Config(signature_version='s3v4'))

        bucket, key = await parse_s3_url(s3_url=shard_file_url)
        return_url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params=dict(Bucket=bucket, Key=key),
            ExpiresIn=SHARD_URL_EXPIRATION_SECONDS
        )
    else:
        return_url = shard_file_url

    return return_url


def get_sha256_hash(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as file:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def get_md5_hash(file_path: str) -> str:
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as file:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file.read(4096), b""):
            md5_hash.update(byte_block)

    return md5_hash.hexdigest()


async def copy_database_file(source_path: str, target_path: str) -> str:
    target_file_name = source_path.split("?")[0].split("/")[-1]
    local_database_file_name = os.path.join(target_path, target_file_name)

    if re.search(r"^https://", source_path):
        await download_file(src=source_path, dst=local_database_file_name)
    else:
        shutil.copy(src=source_path, dst=local_database_file_name)
        logger.info(msg=f"Successfully copied database file: '{source_path}' to path: '{local_database_file_name}'")

    return local_database_file_name
