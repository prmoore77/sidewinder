import asyncio
import websockets
import click
from config import logger
import json
import duckdb
import pyarrow
import base64
from munch import Munch, munchify

# Constants
SUCCESS = "SUCCESS"
FAILED = "FAILED"

# Global
WORKER = Munch(ready=False)


def get_dataframe_bytes(df: pyarrow.Table) -> bytes:
    sink = pyarrow.BufferOutputStream()
    with pyarrow.ipc.new_stream(sink, df.schema) as writer:
        writer.write(df)
    buf = sink.getvalue()
    return buf.to_pybytes()


def get_dataframe_results_as_base64_str(df: pyarrow.Table) -> str:
    return base64.b64encode(get_dataframe_bytes(df)).decode()


db_connection = con = duckdb.connect(database=':memory:')


async def worker():
    async with websockets.connect(uri="ws://localhost:8765/worker",
                                  extra_headers=dict(),
                                  max_size=1024 ** 3
                                  ) as websocket:
        while True:
            message = await websocket.recv()

            if isinstance(message, bytes):
                message = munchify(x=json.loads(message.decode()))
                for table_base64_str in message.table_base64_str_list:
                    df = pyarrow.ipc.open_stream(base64.b64decode(s=table_base64_str)).read_all()
                    table_name = df.schema.metadata[b'name'].decode()
                    db_connection.execute(query=f"CREATE TABLE {table_name} AS SELECT * FROM df")
                    logger.info(msg=f"created table: {table_name}")

                logger.info(msg="All datasets from server created")
                shard_confirmed_dict = dict(kind="ShardConfirmation", shard_id=message.shard_id, successful=True)
                await websocket.send(json.dumps(shard_confirmed_dict).encode())
                logger.info(msg="Sent confirmation to server that worker is ready.")
                WORKER.ready = True
            elif isinstance(message, str):
                logger.info(msg=f"Message from server: {message}")
                message = munchify(x=json.loads(message))

                if message.kind == "Query":
                    if WORKER.ready:
                        sql_command = message.command.rstrip(' ;/')
                        if sql_command:
                            result_dict = None
                            try:
                                df = db_connection.execute(sql_command).fetch_arrow_table().replace_schema_metadata(
                                    metadata=dict(query_id=message.query_id))
                            except Exception as e:
                                result_dict = dict(kind="Result", query_id=message.query_id, status=FAILED, error_message=str(e), results=None)
                            else:
                                result_dict = dict(kind="Result", query_id=message.query_id, status=SUCCESS, error_message=None, results=get_dataframe_results_as_base64_str(df))
                            finally:
                                await websocket.send(json.dumps(result_dict).encode())


@click.command()
def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()
