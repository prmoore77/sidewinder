import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import websockets
import click
import os
from utils import get_dataframe_results_as_base64_str, \
    combine_bytes_results
from config import logger
from utils import coro, get_cpu_count, get_memory_limit, get_dataframe_bytes, duckdb_execute, run_query
import json
import yaml
import uuid
from uuid import UUID
import functools
import duckdb
import base64
from munch import Munch, munchify
from parser.query import Query
import re
from datetime import datetime
import boto3

# Global connection variables
SQL_CLIENT_CONNECTIONS = Munch()
WORKER_CONNECTIONS = Munch()

# Used for collecting info at run time
SHARDS = Munch()
QUERIES = Munch()

# Read our source table shard generation info
with open("config/shard_generation_queries.yaml", "r") as data:
    TABLES = munchify(x=yaml.safe_load(data.read()))

# Query Status Constants
STARTED = "STARTED"
DISTRIBUTED = "DISTRIBUTED"
RUN_ON_SERVER = "RUN_ON_SERVER"
FAILED = "FAILED"
COMPLETED = "COMPLETED"

# Worker Status Constants
WORKER_SUCCESS = "SUCCESS"
WORKER_FAILED = "FAILED"

# Misc. Constants
MAX_WEBSOCKET_MESSAGE_SIZE = 1024**3


async def get_s3_files(shard_data_path):
    s3_client = boto3.client("s3")

    bucket_name = shard_data_path.split("/")[2]
    file_path = "/".join(shard_data_path.split("/")[3:])
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    files = response.get("Contents")

    s3_files = []
    for file in files:
        file_name = file['Key']
        if file_path in file_name and re.search(pattern="\.tar.gz$", string=file_name):
            s3_files.append(f"s3://{bucket_name}/{file_name}")

    return s3_files


async def get_files(shard_data_path):
    dir_list = os.listdir(path=shard_data_path)

    files = []
    for file in dir_list:
        if re.search(pattern="\.tar.gz$", string=file):
            files.append(os.path.join(shard_data_path, file))

    return files


async def get_shards(shard_data_path):
    if re.search(pattern="^s3://", string=shard_data_path):
        shard_files = await get_s3_files(shard_data_path=shard_data_path)
    else:
        shard_files = await get_files(shard_data_path=shard_data_path)

    shards = Munch()
    for shard_file in shard_files:
        shard_id = uuid.uuid4()
        shards[shard_id] = Munch(shard_id=shard_id,
                                 shard_file_name=shard_file,
                                 distributed=False
                                 )

    return shards


class Shard:
    def __init__(self):
        pass


class SidewinderServer:
    def __init__(self,
                 port: int,
                 shard_data_path: str,
                 database_file: str,
                 duckdb_threads: int,
                 duckdb_memory_limit: int,
                 max_process_workers: int,
                 websocket_ping_timeout: int
                 ):
        self.port = port
        self.shard_data_path = shard_data_path
        self.database_file = database_file
        self.duckdb_threads = duckdb_threads
        self.duckdb_memory_limit = duckdb_memory_limit
        self.max_process_workers = max_process_workers
        self.websocket_ping_timeout = websocket_ping_timeout

        # Asynch stuff
        self.event_loop = asyncio.get_event_loop()
        self.event_loop.set_default_executor(ThreadPoolExecutor())
        self.process_pool = ProcessPoolExecutor(max_workers=self.max_process_workers)

        self.shards = self._get_shards()
        self.worker_connections = Munch()
        self.sql_client_connections = Munch()

        self.bound_handler = functools.partial(self.connection_handler)

        async with websockets.serve(ws_handler=self.bound_handler,
                                    host="0.0.0.0",
                                    port=port,
                                    max_size=MAX_WEBSOCKET_MESSAGE_SIZE,
                                    ping_timeout=self.websocket_ping_timeout
                                    ):
            await asyncio.Future()  # run forever

    async def _get_shards(self):
        return None

    async def get_next_shard(self):
        return None

    async def connection_handler(self, websocket):
        if websocket.path == "/client":
            await self.client_handler(client_websocket=websocket)
        elif websocket.path == "/worker":
            await self.worker_handler(worker_websocket=websocket)
        else:
            # No handler for this path; close the connection.
            return

    async def client_handler(self, client_websocket):
        client = SidewinderSQLClient(server=self,
                                     websocket_connection=client_websocket
                                     )
        self.sql_client_connections[client.sql_client_id] = client
        await client.connect()

    async def worker_handler(self, worker_websocket):
        # Get a shard that hasn't been passed out yet...
        shard = await self.get_next_shard()
        worker = SidewinderWorker(server=self,
                                  websocket_connection=worker_websocket,
                                  shard=shard,
                                  event_loop=self.event_loop,
                                  process_pool=self.process_pool,
                                  duckdb_threads=self.duckdb_threads,
                                  duckdb_memory_limit=self.duckdb_memory_limit
                                  )
        self.worker_connections[worker.worker_id] = worker
        await worker.connect()


class SidewinderSQLClient:
    def __init__(self,
                 server: SidewinderServer,
                 websocket_connection
                 ):
        self.server = server
        self.websocket_connection = websocket_connection
        self.sql_client_id = self.websocket_connection.id
        self.distributed_mode = False
        self.summarize_mode = False

    async def connect(self):
        logger.info(
            msg=f"SQL Client Websocket connection: '{self.websocket_connection.id}' - connected")

        await self.websocket_connection.send(f"Client - successfully connected to server - connection ID: '{self.websocket_connection.id}'.")

        await self.process_client_commands()

    async def set_client_attribute(self, message):
        try:
            match = re.search(pattern='^\.set (\S+)\s*=\s*(\S+)\s*$', string=message.rstrip(' ;/'))
            setting = match[1]
            value = match[2].upper()

            if setting == 'distributed':
                distributed_mode = (value == "TRUE")
                await self.websocket_connection.send(
                    f"Distributed set to: {distributed_mode}")
                self.distributed_mode = distributed_mode
            elif setting == "summarize":
                summarize_mode = (value == "TRUE")
                await self.websocket_connection.send(
                    f"Summarize set to: {summarize_mode}")
                self.summarize_mode = summarize_mode
            else:
                raise ValueError(f".set command parameter: {setting} is invalid...")
        except Exception as e:
            await self.websocket_connection.send(
                f".set command failed with error: {str(e)}")

    async def process_client_commands(self):
        try:
            async for message in self.websocket_connection:
                if message:
                    logger.info(msg=f"Message received from SQL client: '{self.sql_client_id}' - '{message}'")

                    if re.search(pattern='^\.set ', string=message):
                        await self.set_client_attribute(message)
                    else:
                        query = SidewinderQuery(sql=message,
                                                client=self
                                                )

                        if query.error_message:
                            await self.websocket_connection.send(
                                f"Query: '{query.query_id}' - failed to parse - error: {query.error_message}")
                        else:
                            distribute_query = None
                            reason = ""
                            comma = ""
                            if self.distributed_mode and query.parsed_query.has_aggregates and len(self.server.worker_connections) > 0:
                                distribute_query = True

                            if len(self.server.worker_connections) == 0:
                                distribute_query = False
                                reason = "There are no workers connected to the server"
                                comma = ", "

                            if not query.parsed_query.has_aggregates:
                                distribute_query = False
                                reason += f"{comma}Query contains no aggregates"
                                comma = ", "

                            if not self.distributed_mode:
                                distribute_query = False
                                reason += f"{comma}Client distributed mode is disabled"

                            if distribute_query:
                                await self.server.distribute_query_to_workers(query)
                            elif not distribute_query:
                                await self.server.run_query_on_server(query=query, reason=reason)

        except websockets.exceptions.ConnectionClosedError:
            pass


class SidewinderQuery:
    def __init__(self,
                 sql: str,
                 client: SidewinderSQLClient,
                 ):
        self.query_id = str(uuid.uuid4())
        self.sql = sql
        self.client = client
        self._parse()
        self.total_workers = 0
        self.completed_workers = 0
        self.start_time = datetime.utcnow().isoformat()
        self.end_time = None
        self.response_sent_to_client = False

        if self.parsed_query:
            self.status = STARTED
        elif self.error_message:
            self.status = FAILED

        self.client.server.queries[self.query_id] = self

    def _parse(self):
        try:
            parsed_query = Query(self.sql.rstrip("/"))
        except Exception as e:
            self.error_message = str(e)
        else:
            self.parsed_query = parsed_query

    def send_results_to_client(self, result_bytes):
        await self.client.websocket_connection.send(result_bytes)
        self.end_time = datetime.utcnow().isoformat()
        await self.client.websocket_connection.send(
            f"Query: '{self.query_id}' - execution elapsed time: {str(datetime.fromisoformat(self.end_time) - datetime.fromisoformat(self.start_time))}"
        )
        logger.info(
            msg=f"Sent Query: '{self.query_id}' results (size: {len(result_bytes)}) to SQL "
                f"Client: '{self.client.sql_client_id}'")
        self.status = COMPLETED
        self.response_sent_to_client = True

    def run_on_server(self, rationale: str):
        await self.client.websocket_connection.send(
            f"Query: '{self.query_id}' - will NOT be distributed - reason: '{rationale}'.  Running server-side...")

        try:
            result_bytes = await self.client.server.event_loop.run_in_executor(
                self.client.server.process_pool,
                run_query,
                self.client.server.database_file,
                self.sql,
                self.client.server.duckdb_threads,
                self.client.server.duckdb_memory_limit
            )
        except Exception as e:
            self.status = FAILED
            self.error_message = str(e)
            await self.client.websocket_connection.send(
                f"Query: {self.query_id} - FAILED on the server - with error: '{self.error_message}'")
            self.response_sent_to_client = True
        else:
            await send_query_results_to_client(result_bytes)

    @property
    async def json(self):
        return "{}"

    async def distribute_to_workers(self):
        for worker_id, worker in self.client.server.worker_connections.items():
            if worker.ready:
                await worker.websocket_connection.send(self.json)
                self.workers[worker_id] = Munch(worker=worker_id, results=None)

        self.status = DISTRIBUTED
        self.total_workers = len(self.workers)

        await self.client.websocket_connection.send(
            f"Query: '{self.query_id}' - distributed to {len(self.workers)} worker(s) by server")


class SidewinderWorker:
    def __init__(self,
                 server: SidewinderServer,
                 websocket_connection,
                 shard: Shard
                 ):
        self.server = server
        self.websocket_connection = websocket_connection
        self.worker_id = self.websocket_connection.id
        self.shard = shard
        self.ready = False

    async def connect(self):
        try:
            logger.info(
                msg=f"Worker Websocket connection: '{self.websocket_connection.id}' - connected")

            logger.info(
                msg=f"Sending info for shard: '{self.shard.shard_id}' ({self.shard}) to worker: '{self.worker_id}'...")

            worker_shard_dict = dict(kind="ShardDataset",
                                     shard_id=str(self.shard.shard_id),
                                     shard_file_name=self.shard.shard_file_name,
                                     worker_id=str(self.worker_id)
                                     )

            worker_shard_message = json.dumps(worker_shard_dict).encode()
            await self.websocket_connection.send(worker_shard_message)
            logger.info(
                msg=f"Sent worker: '{self.worker_id}' - shard info for shard: '{self.shard.shard_id}' - size: {len(worker_shard_message)}")

            await self.collect_results()

        finally:
            logger.warning(msg=f"Worker: '{self.worker_id}' has disconnected.")
            self.shard.distributed = False
            self.shard.confirmed = False
            del self.server.worker_connections[self.worker_id]

    async def collect_results(self):
        try:
            async for message in self.websocket_connection:
                if isinstance(message, bytes):
                    worker_message = Munch(json.loads(message.decode()))
                    logger.info(
                        msg=f"Message (kind={worker_message.kind}) received from Worker: '{self.worker_id}'"
                            f" (shard: '{self.shard_id}') - size: {len(message)}")
                    if worker_message.kind == "ShardConfirmation":
                        logger.info(
                            msg=f"Worker: '{self.worker_id}' has confirmed its shard: '{worker_message.shard_id}' - and "
                                f"is ready to process queries.")
                        self.shard.confirmed = True
                        self.ready = True
                    elif worker_message.kind == "Result":
                        query: SidewinderQuery = self.server.queries[worker_message.query_id]
                        if worker_message.status == WORKER_FAILED:
                            if not query.response_sent_to_client:
                                await query.client.websocket_connection.send(
                                    f"Query: '{query.query_id}' - FAILED with error on worker:"
                                    f" '{self.worker_id}' - with error message: '{worker_message.error_message}'")
                                query.response_sent_to_client = True
                                query.status = FAILED
                        elif worker_message.status == WORKER_SUCCESS:
                            query.workers[str(worker.worker_id)].results = base64.b64decode(worker_message.results)
                            query.completed_workers += 1

                            # If all workers have responded successfully - send the result to the client
                            if query.completed_workers == query.total_workers:
                                results_bytes_list = []
                                for _, worker in query.workers.items():
                                    if worker.results:
                                        results_bytes_list.append(worker.results)
                                        # Free up memory
                                        worker.results = None

                                try:
                                    # Run the Arrow synchronous code in another process to avoid blocking the main thread
                                    # event loop
                                    result_bytes = await self.server.event_loop.run_in_executor(self.server.process_pool,
                                                                                                combine_bytes_results,
                                                                                                results_bytes_list,
                                                                                                query.parsed_query.summary_query,
                                                                                                self.server.duckdb_threads,
                                                                                                self.server.duckdb_memory_limit,
                                                                                                query.client.summarize_mode
                                                                                                )
                                except Exception as e:
                                    query.status = FAILED
                                    query.error_message = str(e)
                                    await query.client.websocket_connection.send(
                                        f"Query: {query.query_id} - succeeded on the worker(s) - but failed to summarize "
                                        f"on the server - with error: '{query.error_message}'")
                                    query.response_sent_to_client = True
                                else:
                                    await query.send_results_to_client(result_bytes=result_bytes)

        except websockets.exceptions.ConnectionClosedError:
            pass


@click.command()
@click.option(
    "--port",
    type=int,
    default=8765,
    show_default=True,
    help="Run the websocket server on this port."
)
@click.option(
    "--shard-data-path",
    type=str,
    default=os.getenv("SHARD_DATA_PATH", "data/shards/tpch_1"),
    show_default=True,
    help="The worker source parquet data file path to use (for shards)."
)
@click.option(
    "--database-file",
    type=str,
    default=os.getenv("DATABASE_FILE", "data/tpch_1.db"),
    show_default=True,
    help="The source parquet data file path to use."
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
    "--max-process-workers",
    type=int,
    default=os.getenv("MAX_PROCESS_WORKERS", get_cpu_count()),
    show_default=True,
    help="Max process workers"
)
@click.option(
    "--websocket-ping-timeout",
    type=int,
    default=os.getenv("PING_TIMEOUT", 60),
    help="Web-socket ping timeout"
)
@coro
async def main(port, database_file, shard_data_path, duckdb_threads, duckdb_memory_limit, max_process_workers,
               websocket_ping_timeout):
    global SHARDS

    logger.info(
        msg=f"Starting Sidewinder Server - (database_file: '{database_file}', shard_data_path: '{shard_data_path}', duckdb_threads: "
            f"{duckdb_threads}, duckdb_memory_limit: {duckdb_memory_limit}b, max_process_workers: {max_process_workers}, websocket_ping_timeout: {websocket_ping_timeout})")
    logger.info(f"Using DuckDB version: {duckdb.__version__}")

    # Initialize our shards
    SHARDS = await get_shards(shard_data_path=shard_data_path)

    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor())
    process_pool = ProcessPoolExecutor(max_workers=max_process_workers)
    bound_handler = functools.partial(handler, loop=loop, process_pool=process_pool, database_file=database_file, duckdb_threads=duckdb_threads, duckdb_memory_limit=duckdb_memory_limit)
    async with websockets.serve(ws_handler=bound_handler,
                                host="0.0.0.0",
                                port=port,
                                max_size=1024 ** 3,
                                ping_timeout=websocket_ping_timeout
                                ):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    main()
