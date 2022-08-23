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

# Constants
SUCCESS = "SUCCESS"
FAILED = "FAILED"

# Global
WORKER = Munch(worker_id=None, ready=False)


async def worker(server_uri, duckdb_threads, duckdb_memory_limit, websocket_ping_timeout, load_duckdb_s3_extension):
    logger.info(msg=(f"Starting Sidewinder Worker - (using: {duckdb_threads} DuckDB thread(s) "
                     f"/ DuckDB memory limit: {duckdb_memory_limit}b "
                     f"/ websocket ping timeout: {websocket_ping_timeout} "
                     f"/ load_duckdb_s3_extension: {load_duckdb_s3_extension})"
                     )
                )
    logger.info(f"Using DuckDB version: {duckdb.__version__}")

    db_connection = duckdb.connect(database=':memory:')
    db_connection.execute(query=f"PRAGMA threads={duckdb_threads}")
    db_connection.execute(query=f"PRAGMA memory_limit='{duckdb_memory_limit}b'")

    if load_duckdb_s3_extension:
        # Setup S3 storage access...
        db_connection.execute(f"INSTALL httpfs")
        db_connection.execute(f"LOAD httpfs")
        db_connection.execute(f"SET s3_region='{os.environ['S3_REGION']}'")
        db_connection.execute(f"SET s3_access_key_id='{os.environ['S3_ACCESS_KEY_ID']}'")
        db_connection.execute(f"SET s3_secret_access_key='{os.environ['S3_SECRET_ACCESS_KEY']}'")
        db_connection.execute(f"SET s3_session_token='{os.environ['S3_SESSION_TOKEN']}'")

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
                    WORKER.worker_id = message.worker_id
                    logger.info(msg=f"Worker ID is: '{WORKER.worker_id}'")
                    logger.info(msg=f"Received shard generation queries for shard: {message.shard_id} - size: {len(raw_message)}")
                    for table in message.shard_query_list:
                        logger.info(msg=f"Executing shard generation query for table: '{table.table_name}' -> \n{table.query}")
                        db_connection.execute(query=table.query)
                        logger.info(msg=f"created table: {table.table_name}")

                    logger.info(msg="Running VACUUM ANALYZE")
                    db_connection.execute(query="VACUUM ANALYZE")

                    logger.info(msg="All datasets from server created")

                    shard_confirmed_dict = dict(kind="ShardConfirmation", shard_id=message.shard_id, successful=True)
                    await websocket.send(json.dumps(shard_confirmed_dict).encode())
                    logger.info(msg=f"Sent confirmation to server that worker: '{WORKER.worker_id}' is ready.")
                    WORKER.ready = True
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
@click.option(
    "--load-duckdb-s3-extension",
    type=bool,
    default=os.getenv("LOAD_DUCKDB_S3_EXTENSION", False),
    help="Set to True to load the DuckDB S3 extension (if you are using s3 paths)"
)
def main(server_uri: str, duckdb_threads: int, duckdb_memory_limit: int, websocket_ping_timeout: int, load_duckdb_s3_extension: bool):
    asyncio.run(worker(server_uri, duckdb_threads, duckdb_memory_limit, websocket_ping_timeout, load_duckdb_s3_extension))


if __name__ == "__main__":
    main()
