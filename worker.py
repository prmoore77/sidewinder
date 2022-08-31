import asyncio
import websockets
import click

from utils import get_dataframe_results_as_base64_str, get_cpu_count, get_memory_limit
from config import logger
import json
import duckdb
import pyarrow
import base64
from munch import Munch, munchify
import os
import psutil
import re
from tempfile import TemporaryDirectory
import shutil
import boto3


# Constants
SUCCESS = "SUCCESS"
FAILED = "FAILED"
CTAS_RETRY_LIMIT = 3

# Global
WORKER = Munch(worker_id=None, ready=False)


async def create_table(db_connection, table):
    logger.info(msg=f"Executing shard generation query for table: '{table.table_name}' -> \n{table.query}")

    db_connection.execute(query=table.query)

    logger.info(msg=f"created table: {table.table_name}")


async def download_s3_file(src: str, dst: str):
    s3_client = boto3.client("s3")

    bucket_name = src.split("/")[2]
    source_file_path = "/".join(src.split("/")[3:])

    logger.info(msg=f"Downloading S3 file: '{src}' - to path: '{dst}'")
    try:
        s3_client.download_file(bucket_name, source_file_path, dst)
    except Exception as e:
        raise
    else:
        logger.info(msg=f"Successfully downloaded S3 file: '{src}' to destination: '{dst}'")


async def copy_database_file(source_path: str, target_path: str) -> str:
    target_file_name = source_path.split("/")[-1]
    local_database_file_name = os.path.join(target_path, target_file_name)

    if re.search("^s3://", source_path):
        await download_s3_file(src=source_path, dst=local_database_file_name)
    else:
        shutil.copy(src=source_path, dst=local_database_file_name)

    logger.info(msg=f"Successfully copied database file: '{source_path}' to path: '{local_database_file_name}'")
    return local_database_file_name


async def get_shard_database(message, websocket, local_database_dir, duckdb_threads, duckdb_memory_limit):
    WORKER.worker_id = message.worker_id
    logger.info(msg=f"Worker ID is: '{WORKER.worker_id}'")
    logger.info(msg=f"Received shard information for shard: '{message.shard_id}'")

    # Get/copy the shard database file...
    local_shard_database_file_name = await copy_database_file(source_path=message.shard_file_name, target_path=local_database_dir)

    try:
        db_connection = duckdb.connect(database=local_shard_database_file_name)
    except Exception as e:
        raise
    else:
        logger.info(msg=f"DuckDB Successfully opened database file: '{local_shard_database_file_name}'")

    db_connection.execute(query=f"PRAGMA threads={duckdb_threads}")
    db_connection.execute(query=f"PRAGMA memory_limit='{duckdb_memory_limit}b'")

    shard_confirmed_dict = dict(kind="ShardConfirmation", shard_id=message.shard_id, successful=True)
    await websocket.send(json.dumps(shard_confirmed_dict).encode())
    logger.info(msg=f"Sent confirmation to server that worker: '{WORKER.worker_id}' is ready.")
    WORKER.ready = True

    return db_connection


async def worker(server_uri, duckdb_threads, duckdb_memory_limit, websocket_ping_timeout):
    logger.info(msg=(f"Starting Sidewinder Worker - (using: {duckdb_threads} DuckDB thread(s) "
                     f"/ DuckDB memory limit: {duckdb_memory_limit}b "
                     f"/ websocket ping timeout: {websocket_ping_timeout})"
                     )
                )
    logger.info(f"Using DuckDB version: {duckdb.__version__}")

    with TemporaryDirectory() as local_database_dir:
        async with websockets.connect(uri=server_uri,
                                      extra_headers=dict(),
                                      max_size=1024 ** 3,
                                      ping_timeout=websocket_ping_timeout
                                      ) as websocket:
            logger.info(msg=f"Successfully connected to server uri: '{server_uri}' - connection: '{websocket.id}'")
            logger.info(msg=f"Waiting to receive data...")
            while True:
                raw_message = await websocket.recv()

                if isinstance(raw_message, bytes):
                    message = munchify(x=json.loads(raw_message.decode()))
                    if message.kind == "ShardDataset":
                        db_connection = await get_shard_database(message=message,
                                                                 websocket=websocket,
                                                                 local_database_dir=local_database_dir,
                                                                 duckdb_threads=duckdb_threads,
                                                                 duckdb_memory_limit=duckdb_memory_limit
                                                                 )
                elif isinstance(raw_message, str):
                    logger.info(msg=f"Message from server: {raw_message}")
                    message = munchify(x=json.loads(raw_message))

                    if message.kind == "Query":
                        if not WORKER.ready:
                            logger.warning(msg=f"Query event ignored - not yet ready to accept queries...")
                        elif WORKER.ready:
                            sql_command = message.command.rstrip(' ;/')
                            if sql_command:
                                result_dict = None
                                try:
                                    df = db_connection.execute(sql_command).fetch_arrow_table().replace_schema_metadata(
                                        metadata=dict(query_id=message.query_id))
                                except Exception as e:
                                    result_dict = dict(kind="Result", query_id=message.query_id, status=FAILED, error_message=str(e), results=None)
                                    logger.error(msg=f"Query: {message.query_id} - Failed - error: {str(e)}")
                                else:
                                    result_dict = dict(kind="Result", query_id=message.query_id, status=SUCCESS, error_message=None, results=get_dataframe_results_as_base64_str(df))
                                    logger.info(msg=f"Query: {message.query_id} - Succeeded - (row count: {df.num_rows} / size: {df.get_total_buffer_size()})")
                                finally:
                                    await websocket.send(json.dumps(result_dict).encode())
                    elif message.kind == "Error":
                        logger.error(msg=f"Server sent an error: {message.error_message}")





@click.command()
@click.option(
    "--server-uri",
    type=str,
    default=os.getenv("SERVER_URL", "ws://localhost:8765/worker"),
    show_default=True,
    help="The server URI to connect to."
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=os.getenv("DUCKDB_THREADS", get_cpu_count()),
    show_default=True,
    help="The number of DuckDB threads to use - default is to use all CPU threads available."
)
@click.option(
    "--duckdb-memory-limit",
    type=int,
    default=os.getenv("DUCKDB_MEMORY_LIMIT", int(0.75 * float(get_memory_limit()))),
    show_default=True,
    help="The amount of memory to allocate to DuckDB - default is to use 75% of physical memory available."
)
@click.option(
    "--websocket-ping-timeout",
    type=int,
    default=os.getenv("PING_TIMEOUT", 60),
    help="Web-socket ping timeout"
)
def main(server_uri: str, duckdb_threads: int, duckdb_memory_limit: int, websocket_ping_timeout: int):
    asyncio.run(worker(server_uri, duckdb_threads, duckdb_memory_limit, websocket_ping_timeout))


if __name__ == "__main__":
    main()
