import json
import os
import platform
import re
import ssl
import sys
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import duckdb
import websockets
import zstandard
from munch import Munch, munchify

from . import __version__ as sidewinder_version
from .config import logger
from .constants import DEFAULT_MAX_WEBSOCKET_MESSAGE_SIZE, SHARD_REQUEST, SHARD_CONFIRMATION, SHARD_DATASET, INFO, QUERY, ERROR, RESULT, WORKER_FAILED, WORKER_SUCCESS, SERVER_PORT, ARROW_RESULT_TYPE
from .utils import coro, pyarrow, get_dataframe_results_as_ipc_base64_str, get_cpu_count, get_memory_limit, copy_database_file, get_sha256_hash, get_md5_hash

# Constants
CTAS_RETRY_LIMIT = 3
SIDEWINDER_WORKER_VERSION = sidewinder_version


class Worker:
    def __init__(self,
                 server_hostname: str,
                 server_port: int,
                 tls_verify: bool,
                 tls_roots: str,
                 mtls: list,
                 mtls_password: str,
                 username: str,
                 password: str,
                 duckdb_threads: int,
                 duckdb_memory_limit: int,
                 websocket_ping_timeout: int,
                 max_websocket_message_size: int
                 ):
        self.server_hostname = server_hostname
        self.server_port = server_port
        self.tls_roots = tls_roots
        self.username = username
        self.password = password
        self.duckdb_threads = duckdb_threads
        self.duckdb_memory_limit = duckdb_memory_limit
        self.websocket_ping_timeout = websocket_ping_timeout
        self.max_websocket_message_size = max_websocket_message_size

        self.worker_id = None
        self.shard_id = None
        self.ready = False
        self.local_database_dir = None
        self.local_shard_database_file_name = None
        self.local_shard_file_sha256_hash = None
        self.local_shard_file_md5_hash = None
        self.db_connection = None

        # Setup TLS/SSL if requested
        self.server_scheme = "wss"

        if mtls:
            self.ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            mtls_cert_chain = mtls[0]
            mtls_private_key = mtls[1]
            self.ssl_context.load_cert_chain(certfile=mtls_cert_chain, keyfile=mtls_private_key, password=mtls_password)
        else:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        self.ssl_context.load_default_certs()

        if tls_verify:
            self.ssl_context.check_hostname = True
            self.ssl_context.verify_mode = ssl.CERT_REQUIRED
            if tls_roots:
                self.ssl_context.load_verify_locations(cafile=self.tls_roots)
        else:
            logger.warning(msg="TLS Verification is disabled.")
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

        # Build the server URI
        self.server_uri = f"{self.server_scheme}://{self.server_hostname}:{self.server_port}/worker"

        self.version = SIDEWINDER_WORKER_VERSION

    async def build_database_from_compressed_tarfile(self,
                                                     compressed_tarfile_name: str
                                                     ):
        logger.info(msg=f"Extract compressed tarfile: '{compressed_tarfile_name}' contents to directory: '{self.local_database_dir}'")

        compressed_tarfile = Path(compressed_tarfile_name)

        with compressed_tarfile.open("rb") as fh:
            dctx = zstandard.ZstdDecompressor()
            with dctx.stream_reader(fh) as reader:
                with tarfile.open(mode="r|", fileobj=reader) as tar:
                    tar.extractall(path=self.local_database_dir)

        # Delete the compressed file..
        compressed_tarfile.unlink()

        database_full_path = next(f.path for f in os.scandir(self.local_database_dir) if f.is_dir())

        database_dir_file_list = os.listdir(path=database_full_path)

        db_connection = duckdb.connect(database=":memory:")

        for file in database_dir_file_list:
            if re.search(pattern=r"\.parquet$", string=file):
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
        logger.info(msg=f"Received shard information for shard: '{self.shard_id}' - details: {message}")

        # Get/copy the shard database file...
        self.local_shard_database_file_name = await copy_database_file(source_path=message.shard_file_name,
                                                                       target_path=self.local_database_dir
                                                                       )

        # Compute a SHA256 hash of the local shard file...
        self.local_shard_file_sha256_hash = get_sha256_hash(file_path=self.local_shard_database_file_name)

        # Compare the hash with the one sent by the server...
        if self.local_shard_file_sha256_hash != message.shard_file_sha256_hash:
            error_message = f"Local shard file SHA256 hash: '{self.local_shard_file_sha256_hash}' does NOT match the server's hash: '{message.shard_file_sha256_hash}'"
            logger.error(msg=error_message)
            raise RuntimeError(error_message)
        else:
            logger.info(msg=f"Local shard file SHA256 hash: '{self.local_shard_file_sha256_hash}' matches the server's hash: '{message.shard_file_sha256_hash}'")

        # Compute an MD5 hash of the local shard file...
        self.local_shard_file_md5_hash = get_md5_hash(file_path=self.local_shard_database_file_name)

        db_connection = await self.build_database_from_compressed_tarfile(compressed_tarfile_name=self.local_shard_database_file_name)

        db_connection.execute(query=f"PRAGMA threads={self.duckdb_threads}")
        db_connection.execute(query=f"PRAGMA memory_limit='{self.duckdb_memory_limit}b'")

        shard_confirmed_dict = dict(kind=SHARD_CONFIRMATION,
                                    shard_id=self.shard_id,
                                    shard_file_md5_hash=self.local_shard_file_md5_hash,
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
                                          ping_timeout=self.websocket_ping_timeout,
                                          ssl=self.ssl_context
                                          ) as self.websocket:
                # Authenticate
                token = f"{self.username}:{self.password}"
                await self.websocket.send(message=token)

                # Request a shard
                shard_request_dict = dict(kind=SHARD_REQUEST)
                await self.websocket.send(message=json.dumps(shard_request_dict).encode())

                logger.info(msg=f"Successfully connected to server uri: '{self.server_uri}' - as user: '{self.username}' - connection: '{self.websocket.id}'")
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

                try:
                    message = munchify(x=json.loads(raw_message))
                except json.decoder.JSONDecodeError:
                    pass
                else:
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
                                       result_type=ARROW_RESULT_TYPE,
                                       results=get_dataframe_results_as_ipc_base64_str(df)
                                       )
                    logger.info(msg=f"Query: {query_message.query_id} - Succeeded - (row count: {df.num_rows} / size: {df.get_total_buffer_size()})")
                finally:
                    await self.websocket.send(json.dumps(result_dict).encode())


@click.command()
@click.option(
    "--version/--no-version",
    type=bool,
    default=False,
    show_default=False,
    required=True,
    help="Prints the Sidewinder Client version and exits."
)
@click.option(
    "--server-hostname",
    type=str,
    default=os.getenv("SERVER_HOSTNAME", "localhost"),
    show_default=True,
    required=True,
    help="The hostname of the Sidewinder server."
)
@click.option(
    "--server-port",
    type=int,
    default=os.getenv("SERVER_PORT", SERVER_PORT),
    show_default=True,
    required=True,
    help="The port of the Sidewinder server."
)
@click.option(
    "--tls-verify/--no-tls-verify",
    type=bool,
    default=(os.getenv("TLS_VERIFY", "TRUE").upper() == "TRUE"),
    show_default=True,
    help="Verify the server's TLS certificate hostname and signature.  Using --no-tls-verify is insecure, only use for development purposes!"
)
@click.option(
    "--tls-roots",
    type=str,
    default=os.getenv("TLS_ROOTS"),
    show_default=True,
    help="Path to trusted TLS certificate(s) - does not apply if '--no-tls-verify' is specified"
)
@click.option(
    "--mtls",
    nargs=2,
    default=None,
    metavar=('CERTFILE', 'KEYFILE'),
    help="Enable transport-level security"
)
@click.option(
    "--mtls-password",
    type=str,
    required=False,
    help="The password for an encrypted client certificate private key (if needed)"
)
@click.option(
    "--username",
    type=str,
    default=os.getenv("WORKER_USERNAME", "worker"),
    show_default=False,
    required=True,
    help="The worker username to authenticate with."
)
@click.option(
    "--password",
    type=str,
    default=os.getenv("WORKER_PASSWORD"),
    show_default=False,
    required=True,
    help="The worker password associated with the username above"
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
async def main(version: bool,
               server_hostname: str,
               server_port: int,
               tls_verify: bool,
               tls_roots: str,
               mtls: list,
               mtls_password: str,
               username: str,
               password: str,
               duckdb_threads: int,
               duckdb_memory_limit: int,
               websocket_ping_timeout: int,
               max_websocket_message_size: int
               ):
    if version:
        print(f"Sidewinder Worker - version: {sidewinder_version}")
        return

    await Worker(server_hostname=server_hostname,
                 server_port=server_port,
                 tls_verify=tls_verify,
                 tls_roots=tls_roots,
                 mtls=mtls,
                 mtls_password=mtls_password,
                 username=username,
                 password=password,
                 duckdb_threads=duckdb_threads,
                 duckdb_memory_limit=duckdb_memory_limit,
                 websocket_ping_timeout=websocket_ping_timeout,
                 max_websocket_message_size=max_websocket_message_size
                 ).run()


if __name__ == "__main__":
    main()
