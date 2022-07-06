import asyncio
import websockets
import click
from config import logger
from utils import coro
import json
import uuid
import functools
import duckdb
import pyarrow



# Global connection variables
SQL_CLIENT_CONNECTIONS = {}
WORKER_CONNECTIONS = {}

QUERIES = {}

db_connection = duckdb.connect(database="data/sidewinder.duckdb")


async def send_query_to_workers(sql_client_websocket):
    try:
        async for message in sql_client_websocket:
            logger.info(msg=f"Message received from SQL client: {sql_client_websocket.id} - '{message}'")

            query_dict = dict(query_id=str(uuid.uuid4()),
                              sql_client_id=str(sql_client_websocket.id),
                              command=message,
                              worker_results=dict()
                              )
            QUERIES[query_dict['query_id']] = query_dict
            for worker_id, worker_websocket in WORKER_CONNECTIONS.items():
                await worker_websocket.send(json.dumps(query_dict))

            await sql_client_websocket.send("Command processed by server")
    except websockets.exceptions.ConnectionClosedError:
        pass


async def sql_client_handler(websocket):
    try:
        SQL_CLIENT_CONNECTIONS[websocket.id] = websocket

        logger.info(
            msg=f"SQL Client Websocket connection: {websocket.id} - connected")

        await websocket.send(f"Client - successfully connected to server - connection ID: {websocket.id}.")

        await send_query_to_workers(websocket)
    finally:
        del SQL_CLIENT_CONNECTIONS[websocket.id]


async def collect_worker_results(worker_websocket):
    try:
        async for message in worker_websocket:
            logger.info(msg=f"Message received from worker: {worker_websocket.id}")

            if isinstance(message, bytes):
                results_df = pyarrow.ipc.open_stream(message).read_all()
                QUERIES[dict['query_id']]['worker_results'][worker_websocket.id] = result_dict

    except websockets.exceptions.ConnectionClosedError:
        pass


async def worker_handler(websocket, shard_count):
    try:
        WORKER_CONNECTIONS[websocket.id] = websocket
        logger.info(
            msg=f"Worker Websocket connection: {websocket.id} - connected")

        df = db_connection.execute(f"SELECT * FROM supplier WHERE MOD(s_suppkey, {shard_count}) = 1").fetch_arrow_table()
        df = df.replace_schema_metadata(metadata=dict(name="supplier"))

        sink = pyarrow.BufferOutputStream()
        with pyarrow.ipc.new_stream(sink, df.schema) as writer:
            writer.write(df)

        buf = sink.getvalue()
        await websocket.send(buf.to_pybytes())

        await collect_worker_results(websocket)
    finally:
        del WORKER_CONNECTIONS[websocket.id]


async def handler(websocket, shard_count):
    if websocket.path == "/client":
        await sql_client_handler(websocket)
    elif websocket.path == "/worker":
        await worker_handler(websocket, shard_count)
    else:
        # No handler for this path; close the connection.
        return


@click.command()
@click.option(
    "--port",
    default=8765,
    show_default=True,
    help="Run the websocket server on this port."
)
@click.option(
    "--shard-count",
    default=10,
    show_default=True,
    help="The number of hash buckets to shard the data to."
)
@coro
async def main(port, shard_count):
    bound_handler = functools.partial(handler, shard_count=shard_count)
    async with websockets.serve(ws_handler=bound_handler, host="localhost", port=port):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    main()
