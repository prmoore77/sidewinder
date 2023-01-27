import json
import os
import platform
import re
import sys
import tarfile
from tempfile import TemporaryDirectory

import click
import duckdb
import mgzip
import websockets
from munch import Munch, munchify

from sidewinder.config import logger
from sidewinder.constants import DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE, SHARD_CONFIRMATION, SHARD_DATASET, INFO, QUERY, ERROR, RESULT, WORKER_FAILED, WORKER_SUCCESS
from sidewinder.utils import coro, pyarrow, get_dataframe_results_as_base64_str, get_cpu_count, get_memory_limit, copy_database_file
from sidewinder import __version__ as sidewinder_version

# Constants
CTAS_RETRY_LIMIT = 3
SIDEWINDER_WORKER_VERSION = sidewinder_version


class Worker:
    def __init__(self,
                 server_uri: str,
                 duckdb_threads: int,
                 duckdb_memory_limit: int,
                 websocket_ping_timeout: int,
                 max_websocket_message_size: int
                 ):
        self.server_uri = server_uri
        self.duckdb_threads = duckdb_threads
        self.duckdb_memory_limit = duckdb_memory_limit
        self.websocket_ping_timeout = websocket_ping_timeout
        self.max_websocket_message_size = max_websocket_message_size

        self.worker_id = None
        self.shard_id = None
        self.ready = False
        self.local_database_dir = None
        self.local_shard_database_file_name = None
        self.db_connection = None

        self.version = SIDEWINDER_WORKER_VERSION

    async def build_database_from_tarfile(self,
                                          tarfile_path: str
                                          ):
        logger.info(msg=f"Extract tarfile: '{tarfile_path}' contents to directory: '{self.local_database_dir}'")
        with mgzip.open(tarfile_path, "rt", thread=self.duckdb_threads) as gz:
            with tarfile.open(name=gz.name) as tar:
                database_dir_name = tar.getmembers()[0].name
                tar.extractall(path=self.local_database_dir)

        database_full_path = os.path.join(self.local_database_dir, database_dir_name)

        database_dir_file_list = os.listdir(path=database_full_path)

        db_connection = duckdb.connect(database=":memory:")

        for file in database_dir_file_list:
            if re.search(pattern="\.parquet$", string=file):
                file_full_path = os.path.join(database_full_path, file)
                table_name = file.split(".")[0]
                sql_text = (f"CREATE VIEW {table_name} AS "
                            f"SELECT * FROM read_parquet('{file_full_path}')"
                            )
                logger.info(msg=f"Executing SQL: '{sql_text}'")
                db_connection.execute(query=sql_text)

        logger.info(msg=f"Successfully opened Database in DuckDB")

        return db_connection

    async def get_shard_database(self,
                                 message: Munch
                                 ):
        self.worker_id = message.worker_id
        logger.info(msg=f"Worker ID is: '{self.worker_id}'")
        self.shard_id = message.shard_id
        logger.info(msg=f"Received shard information for shard: '{self.shard_id}'")

        # Get/copy the shard database file...
        self.local_shard_database_file_name = await copy_database_file(source_path=message.shard_file_name,
                                                                       target_path=self.local_database_dir
                                                                       )

        db_connection = await self.build_database_from_tarfile(tarfile_path=self.local_shard_database_file_name)

        db_connection.execute(query=f"PRAGMA threads={self.duckdb_threads}")
        db_connection.execute(query=f"PRAGMA memory_limit='{self.duckdb_memory_limit}b'")

        shard_confirmed_dict = dict(kind=SHARD_CONFIRMATION,
                                    shard_id=self.shard_id,
                                    successful=True
                                    )
        await self.websocket.send(json.dumps(shard_confirmed_dict).encode())
        logger.info(msg=f"Sent confirmation to server that worker: '{self.worker_id}' is ready.")
        self.ready = True

        return db_connection

    async def run(self):
        logger.info(msg=(f"Starting Sidewinder Worker - version: {self.version} - (\n"
                         f" duckdb_threads: {self.duckdb_threads},\n"
                         f" duckdb_memory_limit: {self.duckdb_memory_limit}b,\n"
                         f" websocket ping timeout: {self.websocket_ping_timeout},\n"
                         f" max_websocket_message_size: {self.max_websocket_message_size}"
                         )
                    )
        logger.info(f"Running on CPU Platform: {platform.machine()}")
        logger.info(f"Using Python version: {sys.version}")
        logger.info(f"Using DuckDB version: {duckdb.__version__}")
        logger.info(f"Using PyArrow version: {pyarrow.__version__}")
        logger.info(f"Using Websockets version: {websockets.__version__}")

        with TemporaryDirectory(dir="/tmp") as self.local_database_dir:
            async with websockets.connect(uri=self.server_uri,
                                          extra_headers=dict(),
                                          max_size=self.max_websocket_message_size,
                                          ping_timeout=self.websocket_ping_timeout
                                          ) as self.websocket:
                logger.info(msg=f"Successfully connected to server uri: '{self.server_uri}' - connection: '{self.websocket.id}'")
                await self.process_server_messages()

    async def process_server_messages(self):
        logger.info(msg=f"Waiting to receive data...")
        while True:
            raw_message = await self.websocket.recv()

            if isinstance(raw_message, bytes):
                message = munchify(x=json.loads(raw_message.decode()))
                if message.kind == SHARD_DATASET:
                    self.db_connection = await self.get_shard_database(message=message)
            elif isinstance(raw_message, str):
                logger.info(msg=f"Message from server: {raw_message}")
                message = munchify(x=json.loads(raw_message))

                if message.kind == INFO:
                    logger.info(msg=f"Informational message from server: '{message.text}'")
                elif message.kind == QUERY:
                    await self.run_query(query_message=message)
                elif message.kind == ERROR:
                    logger.error(msg=f"Server sent an error: {message.error_message}")

    async def run_query(self, query_message: Munch):
        if not self.ready:
            logger.warning(msg=f"Query event ignored - not yet ready to accept queries...")
        elif self.ready:
            sql_command = query_message.command.rstrip(' ;/')
            if sql_command:
                result_dict = None
                try:
                    df = self.db_connection.execute(sql_command).fetch_arrow_table().replace_schema_metadata(
                        metadata=dict(query_id=query_message.query_id))
                except Exception as e:
                    result_dict = dict(kind=RESULT,
                                       query_id=query_message.query_id,
                                       status=WORKER_FAILED,
                                       error_message=str(e),
                                       results=None
                                       )
                    logger.error(msg=f"Query: {query_message.query_id} - Failed - error: {str(e)}")
                else:
                    result_dict = dict(kind=RESULT,
                                       query_id=query_message.query_id,
                                       status=WORKER_SUCCESS,
                                       error_message=None,
                                       results=get_dataframe_results_as_base64_str(df)
                                       )
                    logger.info(msg=f"Query: {query_message.query_id} - Succeeded - (row count: {df.num_rows} / size: {df.get_total_buffer_size()})")
                finally:
                    await self.websocket.send(json.dumps(result_dict).encode())


@click.command()
@click.option(
    "--server-uri",
    type=str,
    default=os.getenv("SERVER_URL", "ws://localhost:8765/worker"),
    show_default=True,
    required=True,
    help="The server URI to connect to."
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=os.getenv("DUCKDB_THREADS", get_cpu_count()),
    show_default=True,
    required=True,
    help="The number of DuckDB threads to use - default is to use all CPU threads available."
)
@click.option(
    "--duckdb-memory-limit",
    type=int,
    default=os.getenv("DUCKDB_MEMORY_LIMIT", int(0.75 * float(get_memory_limit()))),
    show_default=True,
    required=True,
    help="The amount of memory to allocate to DuckDB - default is to use 75% of physical memory available."
)
@click.option(
    "--websocket-ping-timeout",
    type=int,
    default=os.getenv("PING_TIMEOUT", 60),
    show_default=True,
    required=True,
    help="Web-socket ping timeout"
)
@click.option(
    "--max-websocket-message-size",
    type=int,
    default=DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE,
    show_default=True,
    required=True,
    help="Maximum Websocket message size"
)
@coro
async def main(server_uri: str,
               duckdb_threads: int,
               duckdb_memory_limit: int,
               websocket_ping_timeout: int,
               max_websocket_message_size: int
               ):
    await Worker(server_uri=server_uri,
                 duckdb_threads=duckdb_threads,
                 duckdb_memory_limit=duckdb_memory_limit,
                 websocket_ping_timeout=websocket_ping_timeout,
                 max_websocket_message_size=max_websocket_message_size
                 ).run()


if __name__ == "__main__":
    main()
