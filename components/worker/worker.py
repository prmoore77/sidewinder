import asyncio
import websockets
import click

from components.worker.utils import get_dataframe_results_as_base64_str
from config import logger
import json
import duckdb
import pyarrow
import base64
from munch import Munch, munchify
import psutil

# Constants
SUCCESS = "SUCCESS"
FAILED = "FAILED"

# Global
WORKER = Munch(worker_id=None, ready=False)


async def worker(duckdb_threads):
    logger.info(msg=f"Starting Sidewinder worker - (using: {duckdb_threads} DuckDB threads)")

    db_connection = duckdb.connect(database=':memory:')
    db_connection.execute(query=f"PRAGMA threads={duckdb_threads}")

    async with websockets.connect(uri="ws://localhost:8765/worker",
                                  extra_headers=dict(),
                                  max_size=1024 ** 3
                                  ) as websocket:
        logger.info(msg=f"Successfully connected to server - connection: '{websocket.id}'")
        logger.info(msg=f"Waiting to receive data...")
        while True:
            raw_message = await websocket.recv()

            if isinstance(raw_message, bytes):
                message = munchify(x=json.loads(raw_message.decode()))
                if message.kind == "ShardDataset":
                    WORKER.worker_id = message.worker_id
                    logger.info(msg=f"Worker ID is: '{WORKER.worker_id}'")
                    logger.info(msg=f"Received datasets for shard: {message.shard_id} - size: {len(raw_message)}")
                    for table_base64_str in message.table_base64_str_list:
                        df = pyarrow.ipc.open_stream(base64.b64decode(s=table_base64_str)).read_all()
                        table_name = df.schema.metadata[b'name'].decode()
                        db_connection.execute(query=f"CREATE TABLE {table_name} AS SELECT * FROM df")
                        logger.info(msg=f"created table: {table_name}")

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


@click.command()
@click.option(
    "--duckdb-threads",
    type=int,
    default=psutil.cpu_count(),
    show_default=True,
    help="The number of DuckDB threads to use - default is to use all CPU threads available."
)
def main(duckdb_threads: int):
    asyncio.run(worker(duckdb_threads))


if __name__ == "__main__":
    main()
