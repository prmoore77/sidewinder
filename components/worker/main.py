import asyncio
import websockets
import click
from config import logger
import json
import duckdb
import tempfile
import pyarrow


db_connection = con = duckdb.connect(database=':memory:')


async def worker():
    async with websockets.connect(uri="ws://localhost:8765/worker",
                                  extra_headers=dict()
                                  ) as websocket:
        while True:
            message = await websocket.recv()

            if isinstance(message, bytes):
                df = pyarrow.ipc.open_stream(message).read_all()
                table_name = df.schema.metadata[b'name'].decode()
                db_connection.execute(query=f"CREATE TABLE {table_name} AS SELECT * FROM df")
                logger.info(msg=f"created table: {table_name}")
            elif isinstance(message, str):
                logger.info(msg=f"Command from server: {message}")
                message_dict = json.loads(message)
                df = db_connection.execute(message_dict['command'].rstrip(' ;/')).fetch_arrow_table()
                df = df.replace_schema_metadata(metadata=dict(query_id=message_dict['query_id']))

                sink = pyarrow.BufferOutputStream()
                with pyarrow.ipc.new_stream(sink, df.schema) as writer:
                    writer.write(df)

                buf = sink.getvalue()
                await websocket.send(buf.to_pybytes())


@click.command()
def main():
    asyncio.run(worker())


if __name__ == "__main__":
    main()
