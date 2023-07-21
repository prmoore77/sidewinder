import asyncio
import base64
import functools
import json
import os
import platform
import re
import ssl
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from uuid import UUID
from dotenv import load_dotenv

import click
import duckdb
import websockets
from munch import Munch

from . import __version__ as sidewinder_version
from .config import logger
from .constants import SHARD_CONFIRMATION, STARTED, DISTRIBUTED, FAILED, COMPLETED, WORKER_SUCCESS, WORKER_FAILED, DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE, \
    SHARD_DATASET, RESULT, SERVER_PORT, USER_LIST_FILENAME
from .parser.query import Query
from .utils import combine_bytes_results, get_s3_files, get_shard_files, coro, get_cpu_count, get_memory_limit, run_query, pyarrow
from .security import TOKEN_DELIMITER, SECRET_KEY, authenticate_user
from .setup.tls_utilities import DEFAULT_CERT_FILE, DEFAULT_KEY_FILE

# Misc. Constants
SIDEWINDER_SERVER_VERSION = sidewinder_version


class Shard:
    def __init__(self, shard_file_name):
        self.shard_id = uuid.uuid4()
        self.shard_file_name = shard_file_name
        self.distributed = False

    @classmethod
    async def get_shards(cls, shard_data_path):
        if re.search(pattern=r"^s3://", string=shard_data_path):
            shard_files = await get_s3_files(shard_data_path=shard_data_path)
        else:
            shard_files = await get_shard_files(shard_data_path=shard_data_path)

        shards = Munch()
        for shard_file in shard_files:
            shard = cls(shard_file_name=shard_file)
            shards[shard.shard_id] = shard

        logger.info(msg=f"Discovered: {len(shards)} shard(s)...")

        return shards


class SidewinderServer:
    def __init__(self,
                 port: int,
                 tls_certfile: Path,
                 tls_keyfile: Path,
                 mtls_ca_file: Path,
                 user_list_filename: Path,
                 secret_key: str,
                 shard_data_path: str,
                 database_file: str,
                 duckdb_threads: int,
                 duckdb_memory_limit: int,
                 max_process_workers: int,
                 websocket_ping_timeout: int,
                 max_websocket_message_size: int
                 ):
        self.port = port
        self.tls_certfile = tls_certfile
        self.tls_keyfile = tls_keyfile
        self.mtls_ca_file = mtls_ca_file
        self.user_list_filename = user_list_filename
        self.secret_key = secret_key
        self.shard_data_path = shard_data_path
        self.database_file = database_file
        self.duckdb_threads = duckdb_threads
        self.duckdb_memory_limit = duckdb_memory_limit
        self.max_process_workers = max_process_workers
        self.websocket_ping_timeout = websocket_ping_timeout
        self.max_websocket_message_size = max_websocket_message_size

        self.shards = Munch()
        self.worker_connections = Munch()
        self.sql_client_connections = Munch()
        self.queries = Munch()
        self.version = SIDEWINDER_SERVER_VERSION

        # Setup TLS/SSL
        self.ssl_context = None
        if self.tls_certfile and self.tls_keyfile:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            self.ssl_context.load_cert_chain(certfile=self.tls_certfile, keyfile=self.tls_keyfile)

        if self.mtls_ca_file:
            logger.info(msg=f"MTLS is enabled - CA file: {mtls_ca_file}")
            # Enable server-side certificate verification
            self.ssl_context.verify_mode = ssl.CERT_REQUIRED
            self.ssl_context.load_verify_locations(cafile=self.mtls_ca_file)

        # Asynch stuff
        self.event_loop = asyncio.get_event_loop()
        self.event_loop.set_default_executor(ThreadPoolExecutor())
        self.process_pool = ProcessPoolExecutor(max_workers=self.max_process_workers)
        self.bound_handler = functools.partial(self.connection_handler)

    async def run(self):
        self.shards = await Shard.get_shards(shard_data_path=self.shard_data_path)

        logger.info(
            msg=(f"Starting Sidewinder Server - version: {self.version} - (\n"
                 f" port: {self.port},\n"
                 f" tls_certfile: {self.tls_certfile.as_posix() if self.tls_certfile else 'None'},\n"
                 f" tls_keyfile: {self.tls_keyfile.as_posix() if self.tls_keyfile else 'None'},\n"
                 f" mtls_ca_file: {self.mtls_ca_file.as_posix() if self.mtls_ca_file else 'None'},\n"
                 f" user_list_filename: {self.user_list_filename.as_posix()},\n"
                 f" secret_key: (redacted),\n"
                 f" shard_data_path: '{self.shard_data_path}',\n"
                 f" database_file: '{self.database_file}',\n"
                 f" duckdb_threads: {self.duckdb_threads},\n"
                 f" duckdb_memory_limit: {self.duckdb_memory_limit}b,\n"
                 f" max_process_workers: {self.max_process_workers},\n"
                 f" websocket_ping_timeout: {self.websocket_ping_timeout},\n"
                 f" max_websocket_message_size: {self.max_websocket_message_size}\n"
                 f")"
                 )
        )
        logger.info(f"Running on CPU Platform: {platform.machine()}")
        logger.info(f"Using Python version: {sys.version}")
        logger.info(f"Using DuckDB version: {duckdb.__version__}")
        logger.info(f"Using PyArrow version: {pyarrow.__version__}")
        logger.info(f"Using Websockets version: {websockets.__version__}")
        logger.info(f"TLS: {'Enabled' if self.ssl_context else 'Disabled'}")

        async with websockets.serve(ws_handler=self.bound_handler,
                                    host="0.0.0.0",
                                    port=self.port,
                                    max_size=self.max_websocket_message_size,
                                    ping_timeout=self.websocket_ping_timeout,
                                    ssl=self.ssl_context
                                    ):
            await asyncio.Future()  # run forever

    async def get_next_shard(self) -> Shard:
        shard = None
        for shard_id, shard in self.shards.items():
            if not shard.distributed:
                break
        shard.distributed = True
        shard.confirmed = False
        return shard

    async def get_user(self, token: str):
        try:
            username, password = token.split(":")
            # Verify the password
            auth_result = authenticate_user(user_list_filename=self.user_list_filename.as_posix(),
                                            username=username,
                                            password=password,
                                            secret_key=self.secret_key
                                            )
            if auth_result:
                return username
            else:
                return None
        except Exception as e:
            logger.exception(msg=str(e))
            return None

    async def authenticate_socket(self,
                                  websocket_connection):
        token = await websocket_connection.recv()
        user = await self.get_user(token)
        if user is None:
            logger.warning(msg=f"Authentication failed for websocket: '{websocket_connection.id}'")
            await websocket_connection.send("Authentication failed")
            await websocket_connection.close(code=1011, reason="Authentication failed")
            return
        else:
            logger.info(msg=f"User: '{user}' successfully authenticated for websocket: '{websocket_connection.id}'")
            await websocket_connection.send(f"User: '{user}' successfully authenticated to server.")

    async def connection_handler(self, websocket):
        if websocket.path == "/client":
            await self.client_handler(client_websocket=websocket)
        elif websocket.path == "/worker":
            await self.worker_handler(worker_websocket=websocket)
        else:
            # No handler for this path; close the connection.
            return

    async def client_handler(self, client_websocket):
        await self.authenticate_socket(websocket_connection=client_websocket)

        client = SidewinderSQLClient(server=self,
                                     websocket_connection=client_websocket
                                     )
        self.sql_client_connections[client.sql_client_id] = client
        await client.connect()

    async def worker_handler(self, worker_websocket):
        await self.authenticate_socket(websocket_connection=worker_websocket)

        # Get a shard that hasn't been passed out yet...
        shard = await self.get_next_shard()
        worker = SidewinderWorker(server=self,
                                  websocket_connection=worker_websocket,
                                  shard=shard
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
        self.distributed_mode = True
        self.summarize_mode = True

    async def connect(self):
        logger.info(
            msg=f"SQL Client Websocket connection: '{self.websocket_connection.id}' - connected")

        await self.websocket_connection.send((f"Client - successfully connected to Sidewinder server "
                                              f"- version: {self.server.version} "
                                              f"- CPU platform: {platform.machine()} "
                                              f"- TLS: {'Enabled' if self.server.ssl_context else 'Disabled'}"
                                              f"- connection ID: '{self.websocket_connection.id}'."
                                              )
                                             )

        await self.process_client_commands()

    async def set_client_attribute(self, message):
        try:
            match = re.search(pattern=r'^\.set (\S+)\s*=\s*(\S+)\s*$', string=message.rstrip(' ;/'))
            setting = match[1].lower()
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

                    if re.search(pattern=r'^\.set ', string=message):
                        await self.set_client_attribute(message=message)
                    else:
                        query = SidewinderQuery(sql=message,
                                                client=self
                                                )
                        self.server.queries[query.query_id] = query
                        await query.process_query()

        except websockets.exceptions.ConnectionClosedError:
            pass


class SidewinderQuery:
    def __init__(self,
                 sql: str,
                 client: SidewinderSQLClient,
                 ):
        self.query_id = uuid.uuid4()
        self.sql = sql
        self.client = client
        self.workers = Munch()
        self.total_workers = 0
        self.completed_workers = 0
        self.start_time = datetime.utcnow().isoformat()
        self.end_time = None
        self.response_sent_to_client = False
        self.distribute_query = None
        self.distribute_rationale = None

        self.parsed_successfully = None
        self.parsed_query = None
        self.error_message = None
        self._parse()

        if self.parsed_query:
            self.status = STARTED
        elif self.error_message:
            self.status = FAILED

    def _parse(self):
        try:
            parsed_query = Query(self.sql.rstrip("/"))
        except Exception as e:
            self.parsed_successfully = False
            self.error_message = str(e)
        else:
            self.parsed_successfully = True
            self.parsed_query = parsed_query

    async def send_results_to_client(self, result_bytes):
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

    async def run_on_server(self):
        await self.client.websocket_connection.send(
            f"Query: '{self.query_id}' - will NOT be distributed - reason: '{', '.join(self.distribute_rationale)}'.  Running server-side...")

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
            await self.send_results_to_client(result_bytes)

    @property
    async def worker_message_json(self):
        return json.dumps(Munch(kind="Query",
                                query_id=str(self.query_id),
                                sql_client_id=str(self.client.sql_client_id),
                                command=self.sql,
                                status=self.status,
                                start_time=self.start_time
                                )
                          )

    async def distribute_to_workers(self):
        for worker_id, worker in self.client.server.worker_connections.items():
            if worker.ready:
                await worker.websocket_connection.send(await self.worker_message_json)
                self.workers[worker_id] = Munch(worker=worker_id, results=None)

        self.status = DISTRIBUTED
        self.total_workers = len(self.workers)

        await self.client.websocket_connection.send(
            f"Query: '{self.query_id}' - distributed to {len(self.workers)} worker(s) by server")

    async def process_query(self):
        if not self.parsed_successfully:
            await self.client.websocket_connection.send(
                f"Query: '{self.query_id}' - failed to parse - error: {self.error_message}")
        else:
            self.distribute_rationale = []
            if self.client.distributed_mode and self.parsed_query.has_aggregates and len(self.client.server.worker_connections) > 0:
                self.distribute_query = True

            if len(self.client.server.worker_connections) == 0:
                self.distribute_query = False
                self.distribute_rationale.append("There are no workers connected to the server")

            if not self.parsed_query.has_aggregates:
                self.distribute_query = False
                self.distribute_rationale.append("Query contains no aggregates")

            if not self.client.distributed_mode:
                self.distribute_query = False
                self.distribute_rationale.append("Client distributed mode is disabled")

            if self.distribute_query:
                await self.distribute_to_workers()
            elif not self.distribute_query:
                await self.run_on_server()


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

    @property
    async def worker_shard_dict(self):
        return dict(kind=SHARD_DATASET,
                    shard_id=str(self.shard.shard_id),
                    shard_file_name=self.shard.shard_file_name,
                    worker_id=str(self.worker_id)
                    )

    async def connect(self):
        try:
            logger.info(
                msg=f"Worker Websocket connection: '{self.websocket_connection.id}' - connected")

            await self.websocket_connection.send(json.dumps(dict(kind="Info",
                                                                 text=(f"Sidewinder Server - version: {self.server.version} - "
                                                                       f"CPU platform: {platform.machine()} - "
                                                                       f"TLS: {'Enabled' if self.server.ssl_context else 'Disabled'}"
                                                                       )
                                                                 )
                                                            )
                                                 )

            logger.info(
                msg=f"Sending info for shard: '{self.shard.shard_id}' ({self.shard}) to worker: '{self.worker_id}'...")

            worker_shard_message = json.dumps(await self.worker_shard_dict).encode()
            await self.websocket_connection.send(worker_shard_message)
            logger.info(
                msg=f"Sent worker: '{self.worker_id}' - shard info for shard: '{self.shard.shard_id}' - size: {len(worker_shard_message)}")

            await self.process_message()

        finally:
            logger.warning(msg=f"Worker: '{self.worker_id}' has disconnected.")
            self.shard.distributed = False
            self.shard.confirmed = False
            del self.server.worker_connections[self.worker_id]

    async def process_shard_confirmation(self, worker_message: Munch):
        logger.info(
            msg=f"Worker: '{self.worker_id}' has confirmed its shard: '{worker_message.shard_id}' - and "
                f"is ready to process queries.")
        self.shard.confirmed = True
        self.ready = True

    async def process_worker_result(self, worker_message: Munch):
        query: SidewinderQuery = self.server.queries[UUID(worker_message.query_id)]
        if worker_message.status == WORKER_FAILED:
            if not query.response_sent_to_client:
                await query.client.websocket_connection.send(
                    f"Query: '{query.query_id}' - FAILED with error on worker:"
                    f" '{self.worker_id}' - with error message: '{worker_message.error_message}'")
                query.response_sent_to_client = True
                query.status = FAILED
        elif worker_message.status == WORKER_SUCCESS:
            query.workers[self.worker_id].results = base64.b64decode(worker_message.results)
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

    async def process_message(self):
        try:
            async for message in self.websocket_connection:
                if isinstance(message, bytes):
                    worker_message = Munch(json.loads(message.decode()))
                    logger.info(
                        msg=f"Message (kind={worker_message.kind}) received from Worker: '{self.worker_id}'"
                            f" (shard: '{self.shard.shard_id}') - size: {len(message)}")

                    if worker_message.kind == SHARD_CONFIRMATION:
                        await self.process_shard_confirmation(worker_message=worker_message)
                    elif worker_message.kind == RESULT:
                        await self.process_worker_result(worker_message=worker_message)
        except websockets.exceptions.ConnectionClosedError:
            pass


@click.command()
@click.option(
    "--version/--no-version",
    type=bool,
    default=False,
    show_default=False,
    required=True,
    help="Prints the Sidewinder Server version and exits."
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("SERVER_PORT", SERVER_PORT),
    show_default=True,
    required=True,
    help="Run the websocket server on this port."
)
@click.option(
    "--tls",
    nargs=2,
    default=os.getenv("TLS").split(" ") if os.getenv("TLS") else [DEFAULT_CERT_FILE, DEFAULT_KEY_FILE],
    required=False,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security (TLS/SSL).  Provide a Certificate file path, and a Key file path - separated by a space.  Example: tls/server.crt tls/server.key"
)
@click.option(
    "--verify-client/--no-verify-client",
    type=bool,
    default=(os.getenv("VERIFY_CLIENT", "False").upper() == "TRUE"),
    show_default=True,
    required=True,
    help="enable mutual TLS and verify the client if True"
)
@click.option(
    "--mtls",
    type=str,
    default=os.getenv("MTLS"),
    required=False,
    help="If you provide verify-client, you must supply an MTLS CA Certificate file (public key only)"
)
@click.option(
    "--user-list-filename",
    type=str,
    default=USER_LIST_FILENAME,
    show_default=True,
    required=True,
    help="The user dictionary file (in JSON) to use for security - for password-based authentication."
)
@click.option(
    "--secret-key",
    type=str,
    default=SECRET_KEY,
    show_default=False,
    required=True,
    help="The secret key used to salt the user password hashes.  The same key value MUST have been used when creating the user-list-file!"
)
@click.option(
    "--database-file",
    type=str,
    default=os.getenv("DATABASE_FILE", "data/tpch_sf1.duckdb"),
    show_default=True,
    required=True,
    help="The source parquet data file path to use."
)
@click.option(
    "--shard-data-path",
    type=str,
    default=os.getenv("SHARD_DATA_PATH", "data/shards/tpch/sf=1"),
    show_default=True,
    required=True,
    help="The worker source parquet data file path to use (for shards)."
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
    "--max-process-workers",
    type=int,
    default=os.getenv("MAX_PROCESS_WORKERS", get_cpu_count()),
    show_default=True,
    required=True,
    help="Max process workers"
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
async def main(version: bool,
               port: int,
               tls: list,
               verify_client: bool,
               mtls: str,
               user_list_filename: str,
               secret_key: str,
               database_file: str,
               shard_data_path: str,
               duckdb_threads: int,
               duckdb_memory_limit: int,
               max_process_workers: int,
               websocket_ping_timeout: int,
               max_websocket_message_size: int
               ):
    if version:
        print(f"Sidewinder Server - version: {sidewinder_version}")
        return

    tls_certfile = None
    tls_keyfile = None
    if tls:
        tls_certfile = Path(tls[0])
        tls_keyfile = Path(tls[1])

    mtls_ca_file = None
    if verify_client:
        if mtls:
            mtls_ca_file = Path(mtls)

    await SidewinderServer(port=port,
                           tls_certfile=tls_certfile,
                           tls_keyfile=tls_keyfile,
                           mtls_ca_file=mtls_ca_file,
                           user_list_filename=Path(user_list_filename),
                           secret_key=secret_key,
                           shard_data_path=shard_data_path,
                           database_file=database_file,
                           duckdb_threads=duckdb_threads,
                           duckdb_memory_limit=duckdb_memory_limit,
                           max_process_workers=max_process_workers,
                           websocket_ping_timeout=websocket_ping_timeout,
                           max_websocket_message_size=max_websocket_message_size
                           ).run()


if __name__ == "__main__":
    # Load our environment file if it is present
    load_dotenv(dotenv_path=".env")

    main()
