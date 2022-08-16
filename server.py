import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import websockets
import click
import os
from utils import get_dataframe_results_as_base64_str, \
    combine_bytes_results
from config import logger
from utils import coro, get_cpu_limit, get_dataframe_bytes, duckdb_execute, run_query
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


async def distribute_query_to_workers(query, sql_client_websocket):
    workers = Munch()
    for worker_id, worker in WORKER_CONNECTIONS.items():
        if worker.ready:
            await worker.websocket.send(json.dumps(query))
            workers[str(worker_id)] = Munch(worker=worker_id, results=None)

    query.workers = workers
    query.status = DISTRIBUTED
    query.total_workers = len(query.workers)

    await sql_client_websocket.send(
        f"Query: '{query.query_id}' - distributed to {len(query.workers)} worker(s) by server")


async def run_query_on_server(query, sql_client_websocket, loop, process_pool, database_file, duckdb_threads,
                              rationale):
    try:
        await sql_client_websocket.send(
            f"Query: '{query.query_id}' - will NOT be distributed - reason: '{rationale}'.  Running server-side...")

        result_bytes = await loop.run_in_executor(process_pool, run_query,
                                                  database_file,
                                                  query.command,
                                                  duckdb_threads)

        await sql_client_websocket.send(result_bytes)
        logger.info(
            msg=f"Sent Query: '{query.query_id}' results (size: {len(result_bytes)}) to SQL Client: '{query.sql_client_id}'")
        query.status = COMPLETED
        query.response_sent_to_client = True
    except Exception as e:
        query.status = FAILED
        query.error_message = str(e)
        await sql_client_websocket.send(
            f"Query: {query.query_id} - FAILED on the server - with error: '{query.error_message}'")
        query.response_sent_to_client = True


async def set_client_attribute(sql_client_websocket, message):
    try:
        match = re.search('^\.set (\S+)\s*=\s*(\S+)\s*$', message.rstrip(' ;/'))
        setting = match[1]

        if setting == 'distributed':
            value = match[2]
            if value.upper() == "TRUE":
                distributed_mode = True
            else:
                distributed_mode = False

            await sql_client_websocket.send(
                f"Distributed set to: {distributed_mode}")

            setattr(sql_client_websocket, setting, distributed_mode)
        else:
            raise ValueError(f".set command parameter: {setting} is invalid...")
    except Exception as e:
        await sql_client_websocket.send(
            f".set command failed with error: {str(e)}")


async def process_client_commands(sql_client_websocket, loop, process_pool, database_file, duckdb_threads):
    try:
        async for message in sql_client_websocket:
            if message:
                logger.info(msg=f"Message received from SQL client: '{sql_client_websocket.id}' - '{message}'")

                if re.search('^\.set ', message):
                    await set_client_attribute(sql_client_websocket, message)
                else:
                    query_id = str(uuid.uuid4())

                    try:
                        parsed_query = Query(message.rstrip("/"))
                    except Exception as e:
                        await sql_client_websocket.send(
                            f"Query: '{query_id}' - failed to parse - error: {str(e)}")
                    else:
                        query = Munch(kind="Query",
                                      query_id=query_id,
                                      sql_client_id=str(sql_client_websocket.id),
                                      command=message,
                                      workers=Munch(),
                                      total_workers=0,
                                      completed_workers=0,
                                      status=STARTED,
                                      response_sent_to_client=False
                                      )
                        QUERIES[query_id] = query

                        client_distributed_mode = getattr(sql_client_websocket, "distributed", True)

                        distribute_query = None
                        reason = ""
                        comma = ""
                        if client_distributed_mode and parsed_query.has_aggregates and len(WORKER_CONNECTIONS) > 0:
                            distribute_query = True

                        if len(WORKER_CONNECTIONS) == 0:
                            distribute_query = False
                            reason = "There are no workers connected to the server"
                            comma = ", "

                        if not parsed_query.has_aggregates:
                            distribute_query = False
                            reason += f"{comma}Query contains no aggregates"
                            comma = ", "

                        if not client_distributed_mode:
                            distribute_query = False
                            reason += f"{comma}Client distributed mode is disabled"

                        if distribute_query:
                            await distribute_query_to_workers(query, sql_client_websocket)
                        elif not distribute_query:
                            await run_query_on_server(query, sql_client_websocket, loop, process_pool, database_file,
                                                      duckdb_threads, reason)

                        query.parsed_query = parsed_query

    except websockets.exceptions.ConnectionClosedError:
        pass


async def sql_client_handler(websocket, loop, process_pool, database_file, duckdb_threads):
    try:
        SQL_CLIENT_CONNECTIONS[websocket.id] = websocket

        logger.info(
            msg=f"SQL Client Websocket connection: '{websocket.id}' - connected")

        await websocket.send(f"Client - successfully connected to server - connection ID: '{websocket.id}'.")

        await process_client_commands(websocket, loop, process_pool, database_file, duckdb_threads)
    finally:
        logger.info(f"SQL Client Websocket connection: '{websocket.id}' has disconnected.")
        del SQL_CLIENT_CONNECTIONS[websocket.id]


async def collect_worker_results(worker_websocket, loop, process_pool, duckdb_threads):
    try:
        async for message in worker_websocket:
            worker = WORKER_CONNECTIONS[worker_websocket.id]

            if isinstance(message, bytes):
                worker_message = Munch(json.loads(message.decode()))
                logger.info(
                    msg=f"Message (kind={worker_message.kind}) received from Worker: '{worker.worker_id}' (shard: '{worker.shard_id}') - size: {len(message)}")
                if worker_message.kind == "ShardConfirmation":
                    logger.info(
                        msg=f"Worker: '{worker.worker_id}' has confirmed its shard: {worker_message.shard_id} - and is ready to process queries.")
                    shard = SHARDS[worker_message.shard_id]
                    shard.confirmed = True
                    worker.ready = True
                elif worker_message.kind == "Result":
                    query = QUERIES[worker_message.query_id]
                    sql_client_connection = SQL_CLIENT_CONNECTIONS[UUID(query.sql_client_id)]
                    if worker_message.status == WORKER_FAILED:
                        if not query.response_sent_to_client:
                            await sql_client_connection.send(
                                f"Query: '{worker_message.query_id}' - FAILED with error on worker: '{worker.worker_id}' - with error message: '{worker_message.error_message}'")
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
                                # Run the Arrow synchronous code in another process to avoid blocking the main thread event loop
                                result_bytes = await loop.run_in_executor(process_pool, combine_bytes_results,
                                                                          results_bytes_list,
                                                                          query.parsed_query.summary_query,
                                                                          duckdb_threads)
                            except Exception as e:
                                query.status = FAILED
                                query.error_message = str(e)
                                await sql_client_connection.send(
                                    f"Query: {query.query_id} - succeeded on the worker(s) - but failed to summarize on the server - with error: '{query.error_message}'")
                                query.response_sent_to_client = True
                            else:
                                await sql_client_connection.send(result_bytes)
                                logger.info(
                                    msg=f"Sent Query: '{query.query_id}' results (size: {len(result_bytes)}) to SQL Client: '{query.sql_client_id}'")
                                query.status = COMPLETED
                                query.response_sent_to_client = True

    except websockets.exceptions.ConnectionClosedError:
        pass


async def generate_shard_query_list(worker_data_path, shard_count, shard_id, parent_table_name="") -> []:
    shard_query_list = []
    for table in TABLES.tables:
        if table.parent_table_name == parent_table_name:
            format_dict = dict(overall_shard_count=shard_count,
                               shard_id=shard_id,
                               parent_table_dataset=parent_table_name,
                               worker_data_path=worker_data_path
                               )
            table_generation_query = f"CREATE TABLE {table.name} AS {table.query_template.format(**format_dict)}"
            shard_query_list.append(dict(table_name=table.name, query=table_generation_query))

            # Call the routine recursively to get the tree of tables...
            shard_query_list += await generate_shard_query_list(worker_data_path, shard_count, shard_id,
                                                                table.name)

    return shard_query_list


async def get_next_shard():
    shard = None
    for shard_id, shard_value in SHARDS.items():
        if not shard_value.distributed:
            shard = shard_value
            break
    shard.distributed = True
    shard.confirmed = False
    return shard


async def worker_handler(websocket, loop, process_pool, worker_data_path, shard_count, duckdb_threads):
    try:
        logger.info(
            msg=f"Worker Websocket connection: '{websocket.id}' - connected")

        worker = Munch(worker_id=websocket.id, websocket=websocket, ready=False)
        WORKER_CONNECTIONS[worker.worker_id] = worker

        # Get a shard that hasn't been passed out yet...
        shard = await get_next_shard()
        worker.shard_id = shard.shard_id

        logger.info(
            msg=f"Preparing shard: {shard.shard_id} (from worker data path: '{worker_data_path}') for worker: '{worker.worker_id}'...")

        try:
            # Run the Arrow/DuckDB synchronous code in another process to avoid blocking the main thread event loop
            shard_query_list = await generate_shard_query_list(worker_data_path, shard_count, shard.shard_id)
        except Exception as e:
            error_message = f"Server shard query preparation failed for worker: '{worker.worker_id}' - with error: '{str(e)}'"
            await websocket.send(
                json.dumps(dict(kind="Error", error_message=error_message)))
            logger.error(msg=error_message)
            raise
        else:
            worker_shard_dict = dict(kind="ShardDataset",
                                     shard_id=shard.shard_id,
                                     shard_query_list=shard_query_list,
                                     worker_id=str(worker.worker_id)
                                     )

            worker_shard_message = json.dumps(worker_shard_dict).encode()
            await websocket.send(worker_shard_message)
            logger.info(
                msg=f"Sent worker: '{worker.worker_id}' - shard info for shard: {shard.shard_id} - size: {len(worker_shard_message)}")

            await collect_worker_results(websocket, loop, process_pool, duckdb_threads)
    finally:
        logger.warning(msg=f"Worker: '{websocket.id}' has disconnected.")
        del WORKER_CONNECTIONS[websocket.id]


async def handler(websocket, loop, process_pool, database_file, worker_data_path, shard_count, duckdb_threads):
    if websocket.path == "/client":
        await sql_client_handler(websocket, loop, process_pool, database_file, duckdb_threads)
    elif websocket.path == "/worker":
        await worker_handler(websocket, loop, process_pool, worker_data_path, shard_count, duckdb_threads)
    else:
        # No handler for this path; close the connection.
        return


@click.command()
@click.option(
    "--port",
    type=int,
    default=8765,
    show_default=True,
    help="Run the websocket server on this port."
)
@click.option(
    "--worker-data-path",
    type=str,
    default="data/tpch_1",
    show_default=True,
    help="The worker source parquet data file path to use (for shards)."
)
@click.option(
    "--database-file",
    type=str,
    default="data/tpch_1.db",
    show_default=True,
    help="The source parquet data file path to use."
)
@click.option(
    "--shard-count",
    type=int,
    default=10,
    show_default=True,
    help="The number of hash buckets to shard the data to."
)
@click.option(
    "--duckdb-threads",
    type=int,
    default=os.getenv("DUCKDB_THREADS", get_cpu_limit()),
    help="The number of DuckDB threads to use - default is to use all CPU threads available."
)
@click.option(
    "--max-process-workers",
    type=int,
    default=os.getenv("MAX_PROCESS_WORKERS", get_cpu_limit()),
    help="Max process workers"
)
@click.option(
    "--websocket-ping-timeout",
    type=int,
    default=os.getenv("PING_TIMEOUT", 60),
    help="Web-socket ping timeout"
)
@coro
async def main(port, database_file, worker_data_path, shard_count, duckdb_threads, max_process_workers,
               websocket_ping_timeout):
    logger.info(
        msg=f"Starting Sidewinder Server - (database_file: '{database_file}', worker_data_path: '{worker_data_path}', shard_count: {shard_count}, duckdb_threads: {duckdb_threads}, max_process_workers: {max_process_workers}, websocket_ping_timeout: {websocket_ping_timeout})")
    # Initialize our shards
    for i in range(shard_count):
        shard_id = i + 1
        SHARDS[shard_id] = Munch(shard_id=shard_id,
                                 distributed=False
                                 )

    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor())
    process_pool = ProcessPoolExecutor(max_workers=max_process_workers)
    bound_handler = functools.partial(handler, loop=loop, process_pool=process_pool, database_file=database_file,
                                      worker_data_path=worker_data_path,
                                      shard_count=shard_count, duckdb_threads=duckdb_threads)
    async with websockets.serve(ws_handler=bound_handler,
                                host="0.0.0.0",
                                port=port,
                                max_size=1024 ** 3,
                                ping_timeout=websocket_ping_timeout
                                ):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    main()
